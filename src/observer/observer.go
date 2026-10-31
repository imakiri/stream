package observer

type Reporter interface {
	Report(total, delta uint64)
}
