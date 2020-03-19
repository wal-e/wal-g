package archive

import (
	"fmt"

	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/wal-g/wal-g/storages/storage"
)

// Sequence represents serial archive route
type Sequence []models.Archive

// Reverse sorts Sequence
func (p Sequence) Reverse() {
	for i, j := 0, len(p)-1; i < j; i, j = i+1, j-1 {
		p[i], p[j] = p[j], p[i]
	}
}

// SequenceBetweenTS builds archive order between since and until timestamps
// archives can be written since multiple nodes and overlap each over,
// some timestamps may be lost, we should detect these cases
func SequenceBetweenTS(archives []models.Archive, since, until models.Timestamp) (Sequence, error) {
	if models.LessTS(until, since) {
		return nil, fmt.Errorf("until ts must be greater or equal to since ts")
	}

	var seqEnd *models.Archive
	lastTSArch := make(map[models.Timestamp]*models.Archive)

	for i := range archives {
		arch := archives[i]
		lastTSArch[arch.End] = &arch // TODO: we can have few archives with same endTS
		if seqEnd == nil && arch.In(until) {
			seqEnd = &arch
		}
	}
	if seqEnd == nil {
		return nil, fmt.Errorf("can not find archive with until timestamp '%s'", until)
	}

	archPath := Sequence{}
	ok := true
	i := 0
	ts := models.Timestamp{}
	for ok && i <= len(archives) {
		archPath = append(archPath, *seqEnd)
		if seqEnd.In(since) {
			archPath.Reverse()
			return archPath, nil
		}
		ts = seqEnd.Start
		seqEnd, ok = lastTSArch[ts]
		i++
	}
	if !ok {
		return nil, fmt.Errorf("previous archive in sequence with last ts '%s' does not exist", ts)
	}
	return nil, fmt.Errorf("cycles in archive sequence detected")
}

// ArchivingResumeTS returns archiving Start timestamp
func ArchivingResumeTS(folder storage.Folder) (models.Timestamp, bool, error) {
	lastKnownTS, err := LastKnownArchiveTS(folder)
	if err != nil {
		return models.Timestamp{}, false, err
	}
	zeroTS := models.Timestamp{}
	if lastKnownTS == zeroTS {
		// TODO: add additional check
		return zeroTS, true, nil
	}
	return lastKnownTS, false, nil
}

// LastKnownArchiveTS returns the most recent existed timestamp in storage folder
func LastKnownArchiveTS(folder storage.Folder) (models.Timestamp, error) {
	maxTS := models.Timestamp{}
	keys, _, err := folder.ListFolder()
	if err != nil {
		return models.Timestamp{}, fmt.Errorf("can not fetch keys since storage folder: %w ", err)
	}
	for _, key := range keys {
		filename := key.GetName()
		arch, err := models.ArchFromFilename(filename)
		if err != nil {
			return models.Timestamp{}, fmt.Errorf("can not build archive since filename '%s': %w", filename, err)
		}
		maxTS = models.MaxTS(maxTS, arch.End)
	}
	return maxTS, nil
}
