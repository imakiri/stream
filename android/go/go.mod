// Module of the Go side of the Android app, bound with gomobile; its own
// module so golang.org/x/mobile stays out of the main go.mod.
module github.com/imakiri/stream/android/go

go 1.26.0

require github.com/imakiri/stream v0.0.0

require (
	github.com/amnezia-vpn/amneziawg-go v0.2.19 // indirect
	github.com/atotto/clipboard v0.1.4 // indirect
	github.com/beevik/ntp v1.4.3 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/gofrs/uuid/v5 v5.3.2 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/gosuri/uilive v0.0.4 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/reedsolomon v1.12.5 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/mobile v0.0.0-20261011034528-197770444867 // indirect
	golang.org/x/mod v0.42.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/time v0.9.0 // indirect
	golang.org/x/tools v0.52.0 // indirect
	golang.zx2c4.com/wintun v0.0.0-20230126152724-0fa3db229ce2 // indirect
	gvisor.dev/gvisor v0.0.0-20231202080848-1f7806d17489 // indirect
)

replace github.com/imakiri/stream => ../..

tool golang.org/x/mobile/cmd/gobind
