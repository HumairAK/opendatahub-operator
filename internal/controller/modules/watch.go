package modules

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/opendatahub-io/opendatahub-operator/v2/pkg/controller/predicates/dependent"
)

// moduleStatusPredicates returns predicates that include status changes for every module CR.
// it explicitiy add predicates on the CR's status change which can be used for calls to reconcile
// ensure WatchDelete: true, WatchUpdate: true, WatchStatus: true.
func moduleStatusPredicates() map[schema.GroupVersionKind][]predicate.Predicate {
	reg := DefaultRegistry()
	modulesPredicate := make(map[schema.GroupVersionKind][]predicate.Predicate)

	_ = reg.ForAll(func(h ModuleHandler, _ bool) error {
		modulesPredicate[h.GetGVK()] = []predicate.Predicate{
			dependent.New(dependent.WithWatchStatus(true)),
		}
		return nil
	})

	return modulesPredicate
}
