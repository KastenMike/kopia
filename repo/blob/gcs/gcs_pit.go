package gcs

import (
	"context"
	"fmt"
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/pkg/errors"
)

type gcsPointInTimeStorage struct {
	gcsStorage

	pointInTime time.Time
}

func (gcs *gcsPointInTimeStorage) ListBlobs(ctx context.Context, blobIDPrefix blob.ID, cb func(bm blob.Metadata) error) error {
	fmt.Printf("gcsPointInTimeStorage/ListBlobs with prefix: %s\n", blobIDPrefix)
	var (
		previousID blob.ID
		vs         []versionMetadata
	)
	err := gcs.listBlobVersions(ctx, blobIDPrefix, func(vm versionMetadata) error {
		if vm.BlobID != previousID {
			// different blob, process previous one
			if v, found := newestAtUnlessDeleted(vs, gcs.pointInTime); found {
				if err := cb(v.Metadata); err != nil {
					return err
				}
			} else if len(vs) > 0 {
				fmt.Println("skipping as deleted: ", vs[0].BlobID)
			}

			previousID = vm.BlobID
			vs = vs[:0] // reset for next blob
		}

		vs = append(vs, vm)

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "could not list blob versions at time %s", gcs.pointInTime)
	}

	// process last blob
	if v, found := newestAtUnlessDeleted(vs, gcs.pointInTime); found {
		if err := cb(v.Metadata); err != nil {
			return err
		}
	} else if len(vs) > 0 {
		fmt.Println("skipping as deleted: ", vs[0].BlobID)
	}

	return nil
}

func (gcs *gcsPointInTimeStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	// getMetadata returns the specific blob version at time t
	m, err := gcs.getMetadata(ctx, b)
	if err != nil {
		return errors.Wrap(err, "getting metadata")
	}

	fmt.Printf("GetBlob - blob: (%s#%s)\n", b, m.Version)
	return gcs.getBlobWithVersion(ctx, b, m.Version, offset, length, output)
}

func (gcs *gcsPointInTimeStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	bm, err := gcs.getMetadata(ctx, b)

	return bm.Metadata, err
}

func (gcs *gcsPointInTimeStorage) getMetadata(ctx context.Context, b blob.ID) (versionMetadata, error) {
	var vml []versionMetadata

	if err := gcs.getBlobVersions(ctx, b, func(m versionMetadata) error {
		fmt.Printf("getMetadata - found blob (%s#%s). deleted=%t\n", m.Version, m.Timestamp, m.IsDeleteMarker)
		// only include versions older than s.pointInTime
		if !m.Timestamp.After(gcs.pointInTime) {
			fmt.Printf("getMetadata - Skipping version %s with timestamp %s. vs gcs.pointInTime %v\n", m.Version, m.Timestamp, gcs.pointInTime)
			vml = append(vml, m)
		}

		return nil
	}); err != nil {
		return versionMetadata{}, errors.Wrapf(err, "could not get version metadata for blob %s", b)
	}

	if v, found := newestAtUnlessDeleted(vml, gcs.pointInTime); found {
		return v, nil
	}

	return versionMetadata{}, blob.ErrBlobNotFound
}

// newestAtUnlessDeleted returns the last version in the list older than the PIT.
func newestAtUnlessDeleted(vs []versionMetadata, t time.Time) (v versionMetadata, found bool) {
	vs = getOlderThan(vs, t)

	if len(vs) == 0 {
		fmt.Printf("newestAtUnlessDeleted - none found older than timestamp %v\n", t)
		return versionMetadata{}, false
	}

	v = vs[len(vs)-1]
	fmt.Printf("newestAtUnlessDeleted - returning blob: (%s#%s), deleted=%v", v.BlobID, v.Version, v.IsDeleteMarker)

	return v, !v.IsDeleteMarker
}

// Removes versions that are newer than t. The filtering is done in place
// and uses the same slice storage as vs. Assumes entries in vs are in descending
// timestamp order.
func getOlderThan(vs []versionMetadata, t time.Time) []versionMetadata {
	for i := range vs {
		fmt.Printf("getOlderThan - blob (%s#%s) created %s \n", vs[i].BlobID, vs[i].Version, vs[i].Timestamp)
		if vs[i].Timestamp.After(t) {
			fmt.Printf("getOlderThan - Skipping blob (%s#%s) as it's newer than %v\n", vs[i].BlobID, vs[i].Version, t)
			return vs[:i]
		}
	}

	return vs
}

// maybePointInTimeStore wraps s with a point-in-time store when s is versioned
// and a point-in-time value is specified. Otherwise s is returned.
func maybePointInTimeStore(ctx context.Context, gcs *gcsStorage, pointInTime *time.Time) (blob.Storage, error) {
	if pit := gcs.Options.PointInTime; pit == nil || pit.IsZero() {
		return gcs, nil
	}

	fmt.Printf("PIT state with %v\n", *pointInTime)
	// Does the bucket supports versioning?
	attrs, err := gcs.bucket.Attrs(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get determine if bucket '%s' supports versioning", gcs.BucketName)
	}

	if !attrs.VersioningEnabled {
		return nil, errors.Errorf("cannot create point-in-time view for non-versioned bucket '%s'", gcs.BucketName)
	}

	return readonly.NewWrapper(&gcsPointInTimeStorage{
		gcsStorage:  *gcs,
		pointInTime: *pointInTime,
	}), nil
}
