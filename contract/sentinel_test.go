package lock

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hecc-blot/core/enum/errorcode"
)

// TestSentinelsCarryCoreCode 钉住 sentinel → core 码的对应关系。
//
// 这层映射过去"只活在脑子里"（sentinel 与响应码的对应没有任何地方写下来），
// 正是本次改造要消灭的东西 —— 所以它不该只存在于实现里，得有测试守着。
func TestSentinelsCarryCoreCode(t *testing.T) {
	cases := []struct {
		err  error
		want errorcode.Code
	}{
		{ErrLocked, errorcode.CodeConflict},
		{ErrTimeout, errorcode.CodeTimeout},
		{ErrNotHeld, errorcode.CodeIllegalState},
	}

	for _, c := range cases {
		code, ok := errorcode.From(c.err)
		assert.True(t, ok, "%v 未携带 core 码", c.err)
		assert.Equal(t, c.want, code, "%v 的码", c.err)
	}
}

// TestSentinelsRemainIsSable：改为携带码之后，errors.Is 的单例语义必须原样保留 ——
// 这是 A 类 sentinel 存在的全部意义，任何实现变化都不许把它弄坏。
func TestSentinelsRemainIsSable(t *testing.T) {
	wrapped := fmt.Errorf("acquire lock: %w", ErrTimeout)
	assert.True(t, errors.Is(wrapped, ErrTimeout))
	assert.False(t, errors.Is(wrapped, ErrLocked))
}
