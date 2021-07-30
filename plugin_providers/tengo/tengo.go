package yaegi

import (
	"context"
	"fmt"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
	"github.com/whitaker-io/data"
	"github.com/whitaker-io/machine"
)

type tengoProvider struct{}

type tengoSubscription struct {
	read func(ctx context.Context) []data.Data
}

func (t *tengoSubscription) Read(ctx context.Context) []data.Data {
	return t.read(ctx)
}

func (t *tengoSubscription) Close() error {
	return nil
}

type tengoPublisher struct {
	fn func([]data.Data) error
}

func (t *tengoPublisher) Send(d []data.Data) error {
	return t.fn(d)
}

func (t *tengoProvider) Load(pd *machine.PluginDefinition) (interface{}, error) {
	switch pd.Symbol {
	case "subscription":
		return t.Subscription(pd)
	case "applicative":
		return t.Applicative(pd)
	case "comparator":
		return t.Comparator(pd)
	case "fold":
		return t.Fold(pd)
	case "fork":
		return t.Fork(pd)
	case "fork_rule":
		return t.ForkRule(pd)
	case "remover":
		return t.Remover(pd)
	case "publisher":
		return t.Publisher(pd)
	}

	return nil, fmt.Errorf("invalid type %s", pd.Type)
}

func (t *tengoProvider) Subscription(pd *machine.PluginDefinition) (machine.Subscription, error) {
	s := tengo.NewScript([]byte(pd.Payload))
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	// compile the source
	c, err := s.Compile()
	if err != nil {
		return nil, err
	}

	return &tengoSubscription{
		func(ctx context.Context) []data.Data {
			cPrime := c.Clone()
			if err := cPrime.RunContext(ctx); err != nil {
				panic(err)
			}

			arrA := cPrime.Get("a").Array()
			a := make([]data.Data, len(arrA))

			for i, v := range arrA {
				if x, ok := v.(map[string]interface{}); !ok {
					panic(fmt.Errorf("invalid object return in fork %s", pd.Symbol))
				} else {
					a[i] = x
				}
			}

			return a
		},
	}, nil
}

func (t *tengoProvider) Applicative(pd *machine.PluginDefinition) (machine.Applicative, error) {
	s := tengo.NewScript([]byte(pd.Payload))
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	_ = s.Add("data", map[string]interface{}{})

	// compile the source
	c, err := s.Compile()
	if err != nil {
		return nil, err
	}

	return func(d data.Data) error {
		cPrime := c.Clone()
		if err := cPrime.Run(); err != nil {
			return err
		} else if err := cPrime.Set("data", d); err != nil {
			return err
		}

		for k := range d {
			delete(d, k)
		}

		data := cPrime.Get("a").Map()

		for k, v := range data {
			d[k] = v
		}

		return nil
	}, nil
}

func (t *tengoProvider) Comparator(pd *machine.PluginDefinition) (machine.Comparator, error) {
	s := tengo.NewScript([]byte(pd.Payload))
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	_ = s.Add("a", map[string]interface{}{})
	_ = s.Add("b", map[string]interface{}{})

	// compile the source
	c, err := s.Compile()

	if err != nil {
		return nil, err
	}

	return func(a data.Data, b data.Data) int {
		cPrime := c.Clone()
		if err := cPrime.Set("a", a); err != nil {
			panic(err)
		} else if err := cPrime.Set("b", b); err != nil {
			panic(err)
		} else if err := cPrime.Run(); err != nil {
			panic(err)
		}

		return cPrime.Get("compare").Int()
	}, nil
}

func (t *tengoProvider) Fold(pd *machine.PluginDefinition) (machine.Fold, error) {
	s := tengo.NewScript([]byte(pd.Payload))
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	_ = s.Add("aggregate", map[string]interface{}{})
	_ = s.Add("next", map[string]interface{}{})

	// compile the source
	c, err := s.Compile()

	if err != nil {
		return nil, err
	}

	return func(aggregate, next data.Data) data.Data {
		cPrime := c.Clone()
		if err := cPrime.Set("aggregate", aggregate); err != nil {
			panic(err)
		} else if err := cPrime.Set("next", next); err != nil {
			panic(err)
		} else if err := cPrime.Run(); err != nil {
			panic(err)
		}

		return cPrime.Get("aggregate").Map()
	}, nil
}

func (t *tengoProvider) Fork(pd *machine.PluginDefinition) (machine.Fork, error) {
	s := tengo.NewScript([]byte(pd.Payload))
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	_ = s.Add("payload", map[string]interface{}{})

	// compile the source
	c, err := s.Compile()

	if err != nil {
		return nil, err
	}

	return func(payload []*machine.Packet) (a []*machine.Packet, b []*machine.Packet) {
		cPrime := c.Clone()
		if err := cPrime.Set("payload", payload); err != nil {
			panic(err)
		} else if err := cPrime.Run(); err != nil {
			panic(err)
		}

		arrA := cPrime.Get("a").Array()
		arrB := cPrime.Get("b").Array()
		a = make([]*machine.Packet, len(arrA))
		b = make([]*machine.Packet, len(arrB))

		for i, v := range arrA {
			if x, ok := v.(*machine.Packet); !ok {
				panic(fmt.Errorf("invalid object return in fork"))
			} else {
				a[i] = x
			}
		}

		for i, v := range arrB {
			if x, ok := v.(*machine.Packet); !ok {
				panic(fmt.Errorf("invalid object return in fork"))
			} else {
				b[i] = x
			}
		}

		return a, b
	}, nil
}

func (t *tengoProvider) ForkRule(pd *machine.PluginDefinition) (machine.ForkRule, error) {
	s := tengo.NewScript([]byte(pd.Payload))
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	_ = s.Add("data", map[string]interface{}{})

	// compile the source
	c, err := s.Compile()

	if err != nil {
		return nil, err
	}

	return func(d data.Data) bool {
		cPrime := c.Clone()
		if err := cPrime.Set("data", d); err != nil {
			panic(err)
		} else if err := cPrime.Run(); err != nil {
			panic(err)
		}

		return cPrime.Get("compare").Bool()
	}, nil
}

func (t *tengoProvider) Remover(pd *machine.PluginDefinition) (machine.Remover, error) {
	s := tengo.NewScript([]byte(pd.Payload))
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	_ = s.Add("index", 0)
	_ = s.Add("data", map[string]interface{}{})

	// compile the source
	c, err := s.Compile()
	if err != nil {
		return nil, err
	}

	return func(index int, d data.Data) bool {
		cPrime := c.Clone()
		if err := cPrime.Set("index", index); err != nil {
			panic(err)
		} else if err := cPrime.Set("data", d); err != nil {
			panic(err)
		} else if err := cPrime.Run(); err != nil {
			panic(err)
		}

		return cPrime.Get("result").Bool()
	}, nil
}

func (t *tengoProvider) Publisher(pd *machine.PluginDefinition) (machine.Publisher, error) {
	s := tengo.NewScript([]byte(pd.Payload))
	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
	_ = s.Add("data", map[string]interface{}{})

	// compile the source
	c, err := s.Compile()
	if err != nil {
		return nil, err
	}

	return &tengoPublisher{
		func(d []data.Data) error {
			cPrime := c.Clone()
			if err := cPrime.Set("data", d); err != nil {
				return err
			} else if err := cPrime.Run(); err != nil {
				return err
			}

			return cPrime.Get("result").Error()
		},
	}, nil
}

func init() {
	machine.RegisterPluginProvider("tengo", &tengoProvider{})
}
