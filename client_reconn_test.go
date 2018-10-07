// +build test offline

package libmqtt

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestClient_Reconnect(t *testing.T) {
	for _, c := range []Client{plainClient(t, nil), tlsClient(t, nil)} {
		startTime := time.Now()
		once := &sync.Once{}
		var retryCount int32
		c.Connect(func(server string, code byte, err error) {
			if err != nil {
				t.Log("connect to server error", err)
			}

			if code != CodeSuccess {
				t.Log("connect to server failed", code)
			}

			once.Do(func() {
				atomic.StoreInt32(&retryCount, 0)
				time.Sleep(7 * time.Second)
				t.Log("Destroy client")
				c.Destroy(true)
			})
			atomic.AddInt32(&retryCount, 1)
		})
		c.Wait()
		elapsed := time.Now().Sub(startTime)
		t.Log("time used", elapsed)

		if atomic.LoadInt32(&retryCount) != 4 {
			t.Error("retryCount != 4")
		}
		goleak.VerifyNoLeaks(t)
	}
}
