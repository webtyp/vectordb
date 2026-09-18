//go:build !(js && wasm)

package vectordb

func initQuota() {}

func estimateQuotaBytes() int64 {
	return 0
}
