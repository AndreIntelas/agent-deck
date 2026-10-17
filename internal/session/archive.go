package session

import "time"

// IsArchived reports whether the session is in the user archive.
func (i *Instance) IsArchived() bool {
	return i != nil && !i.ArchivedAt.IsZero()
}

// IsArchivedThreadSafe is IsArchived for a reader on another goroutine than
// the one archiving the session, such as the watcher's conductor lookup on its
// relay. Writers that run off the UI goroutine (archive, unarchive, restart
// from the archive, cross-harness switch) set ArchivedAt through
// SetArchivedAtThreadSafe, the other half of the pair.
func (i *Instance) IsArchivedThreadSafe() bool {
	if i == nil {
		return false
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return !i.ArchivedAt.IsZero()
}

// SetArchivedAtThreadSafe sets ArchivedAt under the instance lock. The zero
// time unarchives the session.
func (i *Instance) SetArchivedAtThreadSafe(t time.Time) {
	i.mu.Lock()
	i.ArchivedAt = t
	i.mu.Unlock()
}

// FilterInstancesByArchive returns instances whose archive state matches archived.
func FilterInstancesByArchive(instances []*Instance, archived bool) []*Instance {
	if len(instances) == 0 {
		return nil
	}
	out := make([]*Instance, 0, len(instances))
	for _, inst := range instances {
		if inst == nil {
			continue
		}
		isArchived := !inst.ArchivedAt.IsZero()
		if archived == isArchived {
			out = append(out, inst)
		}
	}
	return out
}

// VisibleInstances returns instances excluding archived rows retained for
// cross-harness recovery (superseded sources) — the canonical tracked set
// that list --json, status --json, TUI header counts and group preview panels
// must all derive from, so aggregates never diverge from the enumerable list.
func VisibleInstances(instances []*Instance) []*Instance {
	visible := make([]*Instance, 0, len(instances))
	for _, inst := range instances {
		if inst != nil && !(inst.IsArchived() && inst.SupersededBy != "") {
			visible = append(visible, inst)
		}
	}
	return visible
}

// SameArchivePartition reports whether two sessions sit in the same archive
// partition — both active, or both archived.
//
// The deck renders exactly one partition at a time: rebuildFlatItems keeps
// only the rows whose IsArchived matches the current view, so an archived
// session is not on screen in the active list and vice versa. Ordering
// operations on GroupTree therefore have to pick their neighbour from the
// same partition as the session being moved; a neighbour from the other
// partition is a row the user cannot see, which makes the move look like it
// did nothing.
func SameArchivePartition(a, b *Instance) bool {
	return a.IsArchived() == b.IsArchived()
}
