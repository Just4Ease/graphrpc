package transport

type Func func(Request) Response

func (f Func) Request(req Request) Response {
	return f(req)
}

func Split(f func(Request) (Transport, error)) Transport {
	return Func(func(req Request) Response {
		tr, err := f(req)
		if err != nil {
			return NewErrorResponse(err)
		}

		return tr.Request(req)
	})
}

// SplitSubscription routes subscription to subtr, and other type of queries to othertr
func SplitSubscription(subtr, othertr Transport) Transport {
	return Split(func(req Request) (Transport, error) {
		if req.Operation == Subscription {
			return subtr, nil
		}

		return othertr, nil
	})
}
