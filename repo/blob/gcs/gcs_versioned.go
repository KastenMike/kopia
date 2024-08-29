package gcs

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"

	"github.com/kopia/kopia/repo/blob"
)

// versionMetadata has metadata for a single BLOB version.
type versionMetadata struct {
	blob.Metadata

	// Versioning related information
	IsDeleteMarker bool
	Version        string
}

// versionMetadataCallback is called when processing the metadata for each blob version.
type versionMetadataCallback func(versionMetadata) error

// getBlobVersions lists all the versions for the blob with the given ID.
func (gcs *gcsPointInTimeStorage) getBlobVersions(ctx context.Context, prefix blob.ID, callback versionMetadataCallback) error {
	var foundBlobs bool

	if err := gcs.list(ctx, prefix, true, func(vm versionMetadata) error {
		foundBlobs = true

		fmt.Printf("Found blobs\n")
		return callback(vm)
	}); err != nil {
		return err
	}

	if !foundBlobs {
		return blob.ErrBlobNotFound
	}

	return nil
}

// listBlobVersions lists all versions for all the blobs with the given blob ID prefix.
func (gcs *gcsPointInTimeStorage) listBlobVersions(ctx context.Context, prefix blob.ID, callback versionMetadataCallback) error {
	return gcs.list(ctx, prefix, false, callback)
}

func (gcs *gcsPointInTimeStorage) list(ctx context.Context, prefix blob.ID, onlyMatching bool, callback versionMetadataCallback) error {
	fmt.Printf("in list per blob.ID: %s\n", prefix)
	query := storage.Query{
		Prefix: gcs.getObjectNameString(prefix),
		// Versions true to output all generations of objects
		Versions: true,
	}

	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	it := gcs.bucket.Objects(ctx, &query)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "could not list objects with prefix %q", query.Prefix)
		}

		if onlyMatching && attrs.Name != query.Prefix {
			return nil
		}

		oi := attrs
		om := gcs.getVersionMetadata(query.Prefix, oi)

		fmt.Printf("found blob: %s -- om.BlobID: %s -- query.Prefix: %s -- prefix: %s\n", oi.Name, om.BlobID, query.Prefix, prefix)

		if errCallback := callback(om); errCallback != nil {
			return errors.Wrapf(errCallback, "callback failed for %q", attrs.Name)
		}
	}

	return nil
}

func toBlobID(blobName, prefix string) blob.ID {
	return blob.ID(strings.TrimPrefix(blobName, prefix))
}

func (gcs *gcsPointInTimeStorage) getVersionMetadata(prefix string, oi *storage.ObjectAttrs) versionMetadata {
	bm := blob.Metadata{
		BlobID:    toBlobID(oi.Name, prefix),
		Length:    oi.Size,
		Timestamp: oi.Created,
	}
	return versionMetadata{
		Metadata:       bm,
		IsDeleteMarker: !oi.Deleted.IsZero() && (gcs.PointInTime == nil || oi.Deleted.Before(*gcs.PointInTime)),
		Version:        strconv.FormatInt(oi.Generation, 10),
	}
}
