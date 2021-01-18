package utils

type subject interface {
	register(Observer observer)
	deregister(Observer observer)
	notifyAll()
}

type observer interface {
	update(string)
	getID() string
}
