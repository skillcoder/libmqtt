/*
 * Copyright Go-IIoT (https://github.com/goiiot)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package libmqtt

import (
	"bytes"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// test with emqttd server (http://emqtt.io/ or https://github.com/emqtt/emqttd)
// the server is configured with default configuration

type extraHandler struct {
	afterPubSuccess   func()
	afterSubSuccess   func()
	afterUnSubSuccess func()
}

func plainClient(t *testing.T, exH *extraHandler) Client {
	c, err := NewClient(
		WithLog(Verbose),
		WithServer("localhost:1883"),
		WithDialTimeout(10),
		WithKeepalive(10, 1.2),
		WithIdentity("admin", "public"),
		WithWill("test", Qos0, false, []byte("test data")),
		WithAutoReconnect(true),
		WithBackoffStrategy(1*time.Second, 5*time.Second, 1.5),
	)

	if err != nil {
		t.Error(err)
	}
	initClient(c, exH, t)
	return c
}

func tlsClient(t *testing.T, exH *extraHandler) Client {
	c, err := NewClient(
		WithLog(Verbose),
		WithServer("localhost:8883"),
		WithTLS(
			"./testdata/client-cert.pem",
			"./testdata/client-key.pem",
			"./testdata/ca-cert.pem",
			"MacBook-Air.local",
			true),
		WithDialTimeout(10),
		WithKeepalive(10, 1.2),
		WithIdentity("admin", "public"),
		WithWill("test", Qos0, false, []byte("test data")),
		WithAutoReconnect(true),
		WithBackoffStrategy(1*time.Second, 5*time.Second, 1.5),
	)

	if err != nil {
		t.Error(err)
	}
	initClient(c, exH, t)
	return c
}

func initClient(c Client, exH *extraHandler, t *testing.T) {
	c.HandlePub(func(topic string, err error) {
		if err != nil {
			t.Error(err)
		}

		if exH != nil && exH.afterPubSuccess != nil {
			exH.afterPubSuccess()
		}
	})

	c.HandleSub(func(topics []*Topic, err error) {
		if err != nil {
			t.Error(err)
		}

		if exH != nil && exH.afterSubSuccess != nil {
			exH.afterSubSuccess()
		}
	})

	c.HandleUnSub(func(topics []string, err error) {
		if err != nil {
			t.Error(err)
		}

		if exH != nil && exH.afterUnSubSuccess != nil {
			exH.afterUnSubSuccess()
		}
	})

	c.HandleNet(func(server string, err error) {
		if err != nil {
			t.Error(err)
		}
	})
}

func testConn(c Client, t *testing.T, afterConnSuccess func()) {
	c.Connect(func(server string, code byte, err error) {
		if err != nil {
			t.Error(err)
		}

		if code != CodeSuccess {
			t.Error(code)
		}

		if afterConnSuccess != nil {
			afterConnSuccess()
		}
	})
}

func testSub(c Client, t *testing.T) {
	c.Handle(testTopics[0], func(topic string, maxQos byte, msg []byte) {
		if maxQos != testPubMsgs[0].Qos || bytes.Compare(testPubMsgs[0].Payload, msg) != 0 {
			t.Error("fail at sub topic =", topic,
				", content unexcepted, payload =", string(msg),
				"target payload =", string(testPubMsgs[0].Payload))
		}
	})

	c.Subscribe(testSubTopics[0])
}

func TestNewClient(t *testing.T) {
	_, err := NewClient()
	if err == nil {
		t.Error(err)
	}

	_, err = NewClient(
		WithTLS(
			"foo",
			"bar",
			"foobar",
			"foo.bar",
			true),
	)
	if err == nil {
		t.Error(err)
	}
}

// conn
func TestClient_Connect(t *testing.T) {
	var c Client
	afterConn := func() {
		c.Destroy(true)
	}

	c = plainClient(t, nil)
	testConn(c, t, afterConn)
	c.Wait()

	c = tlsClient(t, nil)
	testConn(c, t, afterConn)
	c.Wait()

	goleak.VerifyNoLeaks(t)
}

// conn -> pub
func TestClient_Publish(t *testing.T) {
	var c Client
	afterConn := func() {
		c.Publish(testPubMsgs[0])
	}

	exH := &extraHandler{
		afterPubSuccess: func() {
			c.Destroy(true)
		},
	}

	c = plainClient(t, exH)
	testConn(c, t, afterConn)
	c.Wait()

	c = tlsClient(t, exH)
	testConn(c, t, afterConn)
	c.Wait()

	goleak.VerifyNoLeaks(t)
}

// conn -> sub -> pub
func TestClient_Subscribe(t *testing.T) {
	var c Client
	afterConn := func() {
		testSub(c, t)
	}

	extH := &extraHandler{
		afterSubSuccess: func() {
			c.Publish(testPubMsgs[0])
		},
		afterPubSuccess: func() {
			c.Destroy(true)
		},
	}

	c = plainClient(t, extH)
	testConn(c, t, afterConn)
	c.Wait()

	c = tlsClient(t, extH)
	testConn(c, t, afterConn)
	c.Wait()

	goleak.VerifyNoLeaks(t)
}

// conn -> sub -> pub -> unSub
func TestClient_UnSubscribe(t *testing.T) {
	var c Client
	afterConn := func() {
		testSub(c, t)
	}

	extH := &extraHandler{
		afterSubSuccess: func() {
			c.UnSubscribe(testTopics...)
		},
		afterUnSubSuccess: func() {
			c.Destroy(true)
		},
	}

	c = plainClient(t, extH)
	testConn(c, t, afterConn)
	c.Wait()

	c = tlsClient(t, extH)
	testConn(c, t, afterConn)
	c.Wait()

	goleak.VerifyNoLeaks(t)
}
