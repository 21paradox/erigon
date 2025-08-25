package stages

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	networkUnstable atomic.Bool
	resetDuration   = 30 * time.Minute
	once            sync.Once
	resetCh         chan struct{}
)

func init() {
	InitNetworkUnstableDebouncer()
}

func InitNetworkUnstableDebouncer() {
	once.Do(func() {
		resetCh = make(chan struct{}, 1) // 必须初始化
		networkUnstable.Store(false)
		fmt.Println("InitNetworkUnstableDebouncer init")

		go func() {
			timer := time.NewTimer(resetDuration)
			defer timer.Stop()

			for {
				select {
				case <-timer.C:
					networkUnstable.Store(false) // 30 min 到，自动清零

				case <-resetCh:
					networkUnstable.Store(true)
					// 每次调用都“续杯” 30 分钟
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(resetDuration)
				}
			}
		}()
	})
}

func SetNetworkUnstable(unstable bool) {
	if !unstable {
		return
	}
	select {
	case resetCh <- struct{}{}:
	default:
	}
}

func NetworkUnstable() bool {
	return networkUnstable.Load()
}
