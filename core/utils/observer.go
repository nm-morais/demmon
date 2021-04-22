package utils

type Subject interface {
	RegisterObserver(Observer Observer)
	DeregisterObserver(Observer Observer)
	NotifyAll()
}

type Observer interface {
	Notify(interface{})
	ID() string
}

func RemoveFromslice(observerList []Observer, observerToRemove Observer) []Observer {
	observerListLength := len(observerList)
	for i, observer := range observerList {
		if observerToRemove.ID() == observer.ID() {
			observerList[observerListLength-1], observerList[i] = observerList[i], observerList[observerListLength-1]
			return observerList[:observerListLength-1]
		}
	}
	return observerList
}
