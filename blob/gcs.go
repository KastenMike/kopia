package blob

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"time"

	"github.com/skratchdot/open-golang/open"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"google.golang.org/api/googleapi"
	gcsclient "google.golang.org/api/storage/v1"
)

const (
	gcsStorageType   = "gcs"
	gcsTokenCacheDir = ".kopia"

	// Those are not really secret, since the app is installed.
	googleCloudClientID     = "194841383482-nmn10h4mnllnsvou7qr55tfh5jsmtkap.apps.googleusercontent.com"
	googleCloudClientSecret = "ZL52E96Q7iRCD9YXVA7U6UaI"
)

// GCSStorageOptions defines options Google Cloud Storage-backed blob.
type GCSStorageOptions struct {
	// BucketName is the name of the GCS bucket where data is stored.
	BucketName string `json:"bucket"`

	// Prefix specifies additional string to prepend to all objects.
	Prefix string `json:"prefix,omitempty"`

	// TokenCacheFile is the name of the file that will persist the OAuth2 token.
	// If not specified, the token will be persisted in GCSStorageOptions.
	TokenCacheFile string `json:"tokenCacheFile,omitempty"`

	// Token stored the OAuth2 token (when TokenCacheFile is empty)
	Token *oauth2.Token `json:"token,omitempty"`

	// ReadOnly causes the storage to be configured without write permissions, to prevent accidental
	// modifications to the data.
	ReadOnly bool `json:"readonly"`

	// IgnoreDefaultCredentials disables the use of credentials managed by Google Cloud SDK (gcloud).
	IgnoreDefaultCredentials bool `json:"ignoreDefaultCredentials"`
}

func (gso *GCSStorageOptions) ParseURL(u *url.URL) error {
	if u.Scheme != "gcs" {
		return fmt.Errorf("invalid scheme, expected 'file'")
	}

	gso.BucketName = u.Host
	return nil
}

func (gso *GCSStorageOptions) ToURL() *url.URL {
	u := &url.URL{}
	u.Scheme = "gcs"
	u.Host = gso.BucketName
	return u
}

type gcsStorage struct {
	GCSStorageOptions
	objectsService *gcsclient.ObjectsService
}

func (gcs *gcsStorage) BlockExists(b string) (bool, error) {
	_, err := gcs.objectsService.Get(gcs.BucketName, gcs.getObjectNameString(b)).Do()

	if err == nil {
		return true, nil
	}

	return false, err
}

func (gcs *gcsStorage) GetBlock(b string) ([]byte, error) {
	v, err := gcs.objectsService.Get(gcs.BucketName, gcs.getObjectNameString(b)).Download()
	if err != nil {
		if err, ok := err.(*googleapi.Error); ok {
			if err.Code == 404 {
				return nil, ErrBlockNotFound
			}
		}

		return nil, fmt.Errorf("unable to get block '%s': %v", b, err)
	}

	defer v.Body.Close()

	return ioutil.ReadAll(v.Body)
}

func (gcs *gcsStorage) PutBlock(b string, data io.ReadCloser, options PutOptions) error {
	object := gcsclient.Object{
		Name: gcs.getObjectNameString(b),
	}
	defer data.Close()

	call := gcs.objectsService.Insert(gcs.BucketName, &object).Media(data)
	if !options.Overwrite {
		call = call.IfGenerationMatch(0)
	}
	_, err := call.Do()

	return err
}

func (gcs *gcsStorage) DeleteBlock(b string) error {
	err := gcs.objectsService.Delete(gcs.BucketName, string(b)).Do()
	if err != nil {
		return fmt.Errorf("unable to delete block %s: %v", b, err)
	}

	return nil
}

func (gcs *gcsStorage) getObjectNameString(b string) string {
	return gcs.Prefix + string(b)
}

func (gcs *gcsStorage) ListBlocks(prefix string) chan (BlockMetadata) {
	ch := make(chan BlockMetadata, 100)

	go func() {
		ps := gcs.getObjectNameString(prefix)
		page, _ := gcs.objectsService.List(gcs.BucketName).
			Prefix(ps).Do()
		for {
			if page == nil {
				break
			}
			for _, o := range page.Items {
				t, e := time.Parse(time.RFC3339, o.TimeCreated)
				if e != nil {
					ch <- BlockMetadata{
						Error: e,
					}
				} else {
					ch <- BlockMetadata{
						BlockID:   string(o.Name)[len(gcs.Prefix):],
						Length:    o.Size,
						TimeStamp: t,
					}
				}
			}

			if page.NextPageToken != "" {
				page, _ = gcs.objectsService.List(gcs.BucketName).
					PageToken(ps).
					Prefix(gcs.getObjectNameString(prefix)).Do()
			} else {
				break
			}
		}
		close(ch)
	}()

	return ch
}

func (gcs *gcsStorage) Flush() error {
	return nil
}

func (gcs *gcsStorage) Configuration() StorageConfiguration {
	return StorageConfiguration{
		gcsStorageType,
		&gcs.GCSStorageOptions,
	}
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	t := oauth2.Token{}
	err = json.NewDecoder(f).Decode(&t)
	return &t, err
}

func saveToken(file string, token *oauth2.Token) {
	f, err := os.Create(file)
	if err != nil {
		log.Printf("Warning: failed to cache oauth token: %v", err)
		return
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

// NewGCSStorage creates new Google Cloud Storage-backed storage with specified options:
//
// - the 'BucketName' field is required and all other parameters are optional.
//
// By default the connection reuses credentials managed by (https://cloud.google.com/sdk/),
// but this can be disabled by setting IgnoreDefaultCredentials to true.
func NewGCSStorage(options *GCSStorageOptions) (Storage, error) {
	ctx := context.TODO()

	gcs := &gcsStorage{
		GCSStorageOptions: *options,
	}

	if gcs.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	var scope string
	if options.ReadOnly {
		scope = gcsclient.DevstorageReadOnlyScope
	} else {
		scope = gcsclient.DevstorageReadWriteScope
	}

	// Try to get default client if possible and not disabled by options.
	var client *http.Client
	var err error

	if !gcs.IgnoreDefaultCredentials {
		client, _ = google.DefaultClient(context.TODO(), scope)
	}

	if client == nil {
		// Fall back to asking user to authenticate.
		config := &oauth2.Config{
			ClientID:     googleCloudClientID,
			ClientSecret: googleCloudClientSecret,
			Endpoint:     google.Endpoint,
			Scopes:       []string{scope},
		}

		var token *oauth2.Token
		if gcs.Token != nil {
			// Token was provided, use it.
			token = gcs.Token
		} else {
			if gcs.TokenCacheFile == "" {
				// Cache file not provided, token will be saved in storage configuration.
				token, err = tokenFromWeb(ctx, config)
				if err != nil {
					return nil, fmt.Errorf("cannot retrieve OAuth2 token: %v", err)
				}
				gcs.Token = token
			} else {
				token, err = tokenFromFile(gcs.TokenCacheFile)
				if err != nil {
					token, err = tokenFromWeb(ctx, config)
					if err != nil {
						return nil, fmt.Errorf("cannot retrieve OAuth2 token: %v", err)
					}
				}
				saveToken(gcs.TokenCacheFile, token)
			}
		}
		client = config.Client(ctx, token)
	}

	svc, err := gcsclient.New(client)
	if err != nil {
		return nil, fmt.Errorf("Unable to create GCS client: %v", err)
	}

	gcs.objectsService = svc.Objects

	return gcs, nil

}

func readGcsTokenFromFile(filePath string) (*oauth2.Token, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	token := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(token)
	if err != nil {
		return nil, fmt.Errorf("Unable to decode token: %v", err)
	}

	return token, err
}

func writeTokenToFile(filePath string, token *oauth2.Token) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	json.NewEncoder(f).Encode(*token)
	return nil
}

func tokenFromWeb(ctx context.Context, config *oauth2.Config) (*oauth2.Token, error) {
	ch := make(chan string)
	randState := fmt.Sprintf("st%d", time.Now().UnixNano())
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/favicon.ico" {
			http.Error(rw, "", 404)
			return
		}
		if req.FormValue("state") != randState {
			log.Printf("State doesn't match: req = %#v", req)
			http.Error(rw, "", 500)
			return
		}
		if code := req.FormValue("code"); code != "" {
			fmt.Fprintf(rw, "<h1>Success</h1>Authorized.")
			rw.(http.Flusher).Flush()
			ch <- code
			return
		}
		log.Printf("no code")
		http.Error(rw, "", 500)
	}))
	defer ts.Close()

	config.RedirectURL = ts.URL
	authURL := config.AuthCodeURL(randState)
	go openURL(authURL)
	log.Printf("Authorize this app at: %s", authURL)
	code := <-ch
	log.Printf("Got code: %s", code)

	token, err := config.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("token exchange error: %v", err)
	}

	return token, nil
}

func openURL(url string) error {
	return open.Start(url)
}

func authPrompt(url string, state string) (authenticationCode string, err error) {
	ch := make(chan string)

	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/favicon.ico" {
			http.Error(rw, "", 404)
			return
		}
		if req.FormValue("state") != state {
			log.Printf("State doesn't match: req = %#v", req)
			http.Error(rw, "", 500)
			return
		}
		if code := req.FormValue("code"); code != "" {
			fmt.Fprintf(rw, "<h1>Success</h1>Authorized.")
			rw.(http.Flusher).Flush()
			ch <- code
			return
		}
		log.Printf("no code")
		http.Error(rw, "", 500)
	}))
	defer ts.Close()

	log.Println("Go to", url)
	var code string
	n, err := fmt.Scanf("%s", &code)
	if n == 1 {
		return code, nil
	}

	return "", err

}

func init() {
	AddSupportedStorage(
		gcsStorageType,
		func() StorageOptions {
			return &GCSStorageOptions{}
		},
		func(o StorageOptions) (Storage, error) {
			return NewGCSStorage(o.(*GCSStorageOptions))
		})
}
