//go:build js && wasm

package vectordb

import "syscall/js"

func initQuota() {
	nav := js.Global().Get("navigator")
	if nav.IsUndefined() {
		return
	}
	st := nav.Get("storage")
	if st.IsUndefined() {
		return
	}
	persist := st.Get("persist")
	if !persist.IsUndefined() {
		st.Call("persist")
	}
}

func estimateQuotaBytes() int64 {
	return 0
}
