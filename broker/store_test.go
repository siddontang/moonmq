package broker

import (
	"fmt"
	"reflect"
	"testing"
)

func testStore(s Store, checkLen bool) error {
	id1, err := s.GenerateID()
	if err != nil {
		return err
	}

	m1 := newMsg(id1, 1, "abc", []byte("1"))

	id2, err := s.GenerateID()
	if err != nil {
		return err
	}

	m2 := newMsg(id2, 0, "abc", []byte("2"))

	queue := "test_store"

	if err = s.Save(queue, m1); err != nil {
		return err
	}

	if err = s.Save(queue, m2); err != nil {
		return err
	}

	if checkLen {
		if n, err := s.Len(queue); err != nil {
			return err
		} else if n != 2 {
			return fmt.Errorf("%d != 2", n)
		}
	}

	if m, err := s.Front(queue); err != nil {
		return err
	} else if !reflect.DeepEqual(m1, m) {
		return fmt.Errorf("not equal")
	}

	if err := s.Pop(queue); err != nil {
		return err
	}

	if checkLen {
		if n, err := s.Len(queue); err != nil {
			return err
		} else if n != 1 {
			return fmt.Errorf("%d != 1", n)
		}
	}

	if m, err := s.Front(queue); err != nil {
		return err
	} else if !reflect.DeepEqual(m2, m) {
		return fmt.Errorf("not equal")
	}

	if err := s.Delete(queue, m2.id); err != nil {
		return err
	}

	if checkLen {
		if n, err := s.Len(queue); err != nil {
			return err
		} else if n != 0 {
			return fmt.Errorf("%d != 0", n)
		}
	}

	return nil
}

func TestRedisStore(t *testing.T) {
	var config = []byte(`
    {
        "addr":"127.0.0.1:6379",
        "db":0,
        "password":"",
        "idle_conns":16,
        "key_prefix":"test_moonmq"
    }
    `)

	s, err := newRedisStore(config)
	if err != nil {
		t.Fatal(err)
	}

	if err := testStore(s, true); err != nil {
		t.Fatal(err)
	}
}

func TestMemStore(t *testing.T) {
	s, err := newMemStore()
	if err != nil {
		t.Fatal(err)
	}

	if err := testStore(s, true); err != nil {
		t.Fatal(err)
	}
}

// func TestLevelDBStore(t *testing.T) {
// 	var config = []byte(`
//     {
//         "path" : "./testdb",
//         "compression":true,
//         "block_size" : 32,
//         "write_buffer_size" : 2,
//         "cache_size" : 20,
//         "key_prefix" : "test_moonmq"
//     }
//     `)

// 	s, err := newLevelDBStore(config)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	defer s.db.Destroy()

// 	if err := testStore(s, false); err != nil {
// 		t.Fatal(err)
// 	}
// }
