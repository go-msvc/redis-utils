module github.com/go-msvc/redis-utils

go 1.18

replace github.com/go-msvc/utils => ../utils

replace github.com/go-msvc/utils/ms => ../utils/ms

replace github.com/go-msvc/utils/keyvalue => ../utils/keyvalue

require (
	github.com/go-msvc/errors v1.2.0
	github.com/go-msvc/nats-utils v0.0.0-20230212131622-3fa4c2461402
	github.com/go-msvc/utils v0.0.0-20230117192331-a72280d31e5a
)

require (
	github.com/go-msvc/config v0.0.2 // indirect
	github.com/go-msvc/data v1.0.1 // indirect
	github.com/go-msvc/logger v1.0.0 // indirect
	github.com/mediocregopher/radix/v3 v3.8.1 // indirect
	golang.org/x/xerrors v0.0.0-20191011141410-1b5146add898 // indirect
)
