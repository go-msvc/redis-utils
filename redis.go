package redis

// type redisHandler struct {
// 	redis             *redis.Client
// 	subscriptionsMu   sync.Mutex
// 	subscriptions     map[string]redisSub
// 	replySub          string
// 	replySubscription redisSub
// 	replyHandlerMu    sync.Mutex
// 	replyHandlerFunc  ReplyHandler
// } // redisHandler

// type redisSub struct {
// 	done *bool
// }

// // newRedis creates a new Redis Comms handler
// func newRedis(config redisConfig, opts *options) (handler, error) {

// 	var r redisHandler

// 	if err := r.init(config, opts); err != nil {
// 		return nil, errors.Wrapf(err,
// 			"Failed to init Redis client")
// 	} // if failed to init

// 	return &r, nil

// } // newRedis()

// // init initialises the Redis Comms handler
// func (r *redisHandler) init(config redisConfig, opts *options) error {

// 	const method = "redisHandler.init"

// 	if r == nil || opts == nil {
// 		return errors.Errorf("Invalid parameters %p.%s ()",
// 			r,
// 			method)
// 	} // if invalid params

// 	if err := validate.Validate(
// 		&config); err != nil {
// 		return errors.Wrapf(err,
// 			"Config invalid")
// 	} // if invalid

// 	var err error

// 	if r.redis, err = redis.NewClient(
// 		config.Client); err != nil {

// 		return errors.Wrapf(err,
// 			"Failed to create Redis client")

// 	} // if failed to init

// 	r.subscriptions = make(map[string]redisSub)
// 	r.replySub = "QR:" + uuid.NewString()

// 	return nil

// } // redisHandler.init()

// // send is an implementation of handler
// func (r *redisHandler) send(subject string, msgData []byte, opts *sendOptions) error {

// 	const method = "redisHandler.send"

// 	if r == nil || len(subject) <= 0 || msgData == nil {
// 		return errors.Errorf("Invalid parameters %p.%s ()",
// 			r,
// 			method)
// 	} // if invalid params

// 	logger.Debugf("Pushing message on %s",
// 		subject)

// 	listLen, err := r.redis.LPush(
// 		subject,
// 		msgData)

// 	if err != nil {
// 		return errors.Wrapf(err,
// 			"Failed to push message on queue %s",
// 			subject)
// 	} // if failed to push

// 	logger.Debugf("Pushed message on %s. List length %d",
// 		subject,
// 		listLen)

// 	return nil

// } // redisHandler.send()

// subscribe is an implementation of handler
// func (r *redisHandler) subscribe(subject string, callback MessageHandler) error {

// 	const method = "redisHandler.subscribe"

// 	if r == nil || len(subject) <= 0 || callback == nil {
// 		return errors.Errorf("Invalid parameters %p.%s ()",
// 			r,
// 			method)
// 	} // if invalid params

// 	r.subscriptionsMu.Lock()
// 	defer r.subscriptionsMu.Unlock()

// 	if _, ok := r.subscriptions[subject]; ok {
// 		return errors.Errorf(
// 			"Already subscribed to subject %s",
// 			subject)
// 	} // if subscribed

// 	sub, err := r.sub(
// 		subject,
// 		callback)

// 	if err != nil {
// 		return errors.Wrapf(err,
// 			"Failed to subscribe to %s",
// 			subject)
// 	} // if failed to sub

// 	r.subscriptions[subject] = sub
// 	return nil

// } // redisHandler.subscribe()

// func (r *redisHandler) sub(subject string, callback MessageHandler) (redisSub, error) {

// 	var done bool

// 	for i := 0; i < 2; i++ {

// 		go func() {

// 			for {

// 				if done {
// 					break
// 				} // if done

// 				_, value, err := r.redis.BRPop(
// 					time.Second*2,
// 					subject)

// 				if done {
// 					break
// 				} // if done

// 				if err != nil {
// 					err := errors.Wrapf(err,
// 						"Failed to pop value")
// 					logger.Errorf("%+v", err)
// 					time.Sleep(time.Second)
// 					continue
// 				} // if failed to pop

// 				if len(value) <= 0 {
// 					logger.Tracef("No value received")
// 					continue
// 				} // if not value

// 				callback(value, "", nil)

// 			} // forever

// 		}() // func ()

// 	} // for each go routine

// 	return redisSub{
// 		done: &done,
// 	}, nil

// } // redisHandler.sub()

// // unSubscribe is an implementation of handler
// func (r *redisHandler) unSubscribe(subject string) error {

// 	r.subscriptionsMu.Lock()
// 	defer r.subscriptionsMu.Unlock()

// 	if err := r.unSub(subject); err != nil {
// 		return errors.Wrapf(err,
// 			"Failed to unsubscribe from %s",
// 			subject)
// 	} // if failed to unsub

// 	return nil

// } // redisHandler.unSubscribe()

// func (r *redisHandler) unSub(subject string) error {

// 	sub, ok := r.subscriptions[subject]
// 	if !ok {
// 		return nil
// 	} // not subscribed

// 	*sub.done = true
// 	delete(r.subscriptions, subject)
// 	return nil

// } // redisHandler.unSub()

// // unSubscribeAll is an implementation of handler
// func (r *redisHandler) unSubscribeAll() error {

// 	r.subscriptionsMu.Lock()
// 	defer r.subscriptionsMu.Unlock()

// 	var merr errors.MultiError

// 	for subject := range r.subscriptions {
// 		merr.Append(r.unSub(subject))
// 	} // for each subject

// 	if merr != nil {
// 		return errors.Wrapf(merr,
// 			"Failed to unsubscribe all")
// 	} // if failed

// 	return nil

// } // redisHandler.unSubscribeAll()

// // replyHandler is an implementation of handler
// func (r *redisHandler) replyHandler(callback ReplyHandler) error {

// 	r.replyHandlerMu.Lock()
// 	defer r.replyHandlerMu.Unlock()

// 	if r.replyHandlerFunc != nil {
// 		return errors.Errorf(
// 			"Reply handler already set")
// 	} // if set
// 	r.replyHandlerFunc = callback

// 	var err error
// 	r.replySubscription, err = r.sub(
// 		r.replySub,
// 		func(msg []byte, subject string, headers map[string][]string) {
// 			r.replyHandlerFunc(msg, subject, headers)
// 		},
// 	)

// 	if err != nil {
// 		return errors.Wrapf(err,
// 			"Failed to subscribe to %s",
// 			r.replySub)
// 	} // if failed to subscribe

// 	return nil

// } // redisHandler.replyHandler()

// // replySubject is an implementation of handler
// func (r *redisHandler) replySubject() Subject {
// 	return Subject(r.replySub)
// } // redisHandler.replySubject()
