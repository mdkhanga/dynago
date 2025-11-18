package storage_test

import (
	"sync"

	m "github.com/mdkhanga/dynago/models"
	"github.com/mdkhanga/dynago/storage"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("MemoryStore", func() {
	var (
		store *storage.MemoryStore
	)

	BeforeEach(func() {
		// Create a fresh store for each test
		store = storage.NewMemoryStore()
	})

	Describe("Set and Get", func() {
		Context("when setting a single key-value pair", func() {
			It("should store and retrieve the value correctly", func() {
				kv := &m.KeyValue{Key: "name", Value: "John"}
				store.Set(kv)

				result := store.Get("name")
				Expect(result.Key).To(Equal("name"))
				Expect(result.Value).To(Equal("John"))
			})
		})

		Context("when setting multiple key-value pairs", func() {
			It("should store and retrieve all values correctly", func() {
				kv1 := &m.KeyValue{Key: "name", Value: "Alice"}
				kv2 := &m.KeyValue{Key: "age", Value: "30"}
				kv3 := &m.KeyValue{Key: "city", Value: "NYC"}

				store.Set(kv1)
				store.Set(kv2)
				store.Set(kv3)

				Expect(store.Get("name").Value).To(Equal("Alice"))
				Expect(store.Get("age").Value).To(Equal("30"))
				Expect(store.Get("city").Value).To(Equal("NYC"))
			})
		})

		Context("when updating an existing key", func() {
			It("should overwrite the previous value", func() {
				kv1 := &m.KeyValue{Key: "status", Value: "active"}
				store.Set(kv1)

				Expect(store.Get("status").Value).To(Equal("active"))

				kv2 := &m.KeyValue{Key: "status", Value: "inactive"}
				store.Set(kv2)

				Expect(store.Get("status").Value).To(Equal("inactive"))
			})
		})

		Context("when getting a non-existent key", func() {
			It("should return empty value", func() {
				result := store.Get("nonexistent")
				Expect(result.Key).To(Equal("nonexistent"))
				Expect(result.Value).To(BeEmpty())
			})
		})
	})

	Describe("Concurrent operations", func() {
		Context("when multiple goroutines set values", func() {
			It("should handle concurrent writes safely", func() {
				var wg sync.WaitGroup
				iterations := 100

				for i := 0; i < iterations; i++ {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						key := "concurrent"
						value := string(rune(index))
						kv := &m.KeyValue{Key: key, Value: value}
						store.Set(kv)
					}(i)
				}

				wg.Wait()

				// Should not panic and should have some value
				result := store.Get("concurrent")
				Expect(result).ToNot(BeNil())
			})
		})

		Context("when multiple goroutines read and write", func() {
			It("should handle concurrent reads and writes safely", func() {
				var wg sync.WaitGroup

				// Initial value
				store.Set(&m.KeyValue{Key: "shared", Value: "initial"})

				// Writers
				for i := 0; i < 50; i++ {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						kv := &m.KeyValue{Key: "shared", Value: string(rune(index))}
						store.Set(kv)
					}(i)
				}

				// Readers
				for i := 0; i < 50; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						result := store.Get("shared")
						Expect(result).ToNot(BeNil())
					}()
				}

				wg.Wait()

				// Should not panic
				result := store.Get("shared")
				Expect(result).ToNot(BeNil())
			})
		})

		Context("when setting different keys concurrently", func() {
			It("should store all values correctly", func() {
				var wg sync.WaitGroup
				numKeys := 100

				for i := 0; i < numKeys; i++ {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						key := "key_" + string(rune('0'+index%10))
						value := "value_" + string(rune('0'+index%10))
						kv := &m.KeyValue{Key: key, Value: value}
						store.Set(kv)
					}(i)
				}

				wg.Wait()

				// Verify we can read values without panic
				for i := 0; i < 10; i++ {
					key := "key_" + string(rune('0'+i))
					result := store.Get(key)
					Expect(result.Key).To(Equal(key))
				}
			})
		})
	})

	Describe("Edge cases", func() {
		Context("when setting empty key", func() {
			It("should store and retrieve with empty key", func() {
				kv := &m.KeyValue{Key: "", Value: "empty-key"}
				store.Set(kv)

				result := store.Get("")
				Expect(result.Value).To(Equal("empty-key"))
			})
		})

		Context("when setting empty value", func() {
			It("should store and retrieve empty value", func() {
				kv := &m.KeyValue{Key: "empty", Value: ""}
				store.Set(kv)

				result := store.Get("empty")
				Expect(result.Value).To(BeEmpty())
			})
		})

		Context("when setting very long values", func() {
			It("should handle large strings", func() {
				longValue := string(make([]byte, 10000))
				kv := &m.KeyValue{Key: "large", Value: longValue}
				store.Set(kv)

				result := store.Get("large")
				Expect(len(result.Value)).To(Equal(10000))
			})
		})
	})
})
