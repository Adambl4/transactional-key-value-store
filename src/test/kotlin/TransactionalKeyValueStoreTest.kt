import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import kotlin.test.BeforeTest

class TransactionalKeyValueStoreTest {
    private lateinit var kvStore: ITransactionalKeyValueStore

    private fun buildKeyValueStore() = TransactionalKeyValueStore(
        store = InMemoryKeyValueStore()
    )

    @BeforeTest
    fun setUp() {
        kvStore = buildKeyValueStore()
    }

    @Test
    fun testSingleTransactionSet() {
        kvStore.inTransaction {
            set("key", "value")
        }
        assertEquals("value", kvStore["key"])
    }

    @Test
    fun testSingleTransactionGet() {
        kvStore.inTransaction {
            set("key", "value")
        }
        kvStore.inTransaction {
            assertEquals("value", get("key"))
        }
    }

    @Test
    fun testSingleTransactionDelete() {
        kvStore.inTransaction {
            set("key", "value")
            delete("key")
        }
        assertNull(kvStore["key"])
    }

    @Test
    fun testSingleTransactionGetAll() {
        kvStore.inTransaction {
            set("key1", "value1")
            set("key2", "value2")
        }
        assertEquals(mapOf("key1" to "value1", "key2" to "value2"), kvStore.getAll())
    }

    @Test
    fun testNestedTransactionSet() {
        kvStore.inTransaction {
            set("key", "value1")
            inTransaction {
                set("key", "value2")
            }
        }
        assertEquals("value2", kvStore["key"])
    }

    @Test
    fun testNestedTransactionGet() {
        kvStore.inTransaction {
            set("key", "value1")
            inTransaction {
                assertEquals("value1", get("key"))
            }
        }
    }

    @Test
    fun testNestedTransactionDelete() {
        kvStore.inTransaction {
            set("key", "value")
            inTransaction {
                delete("key")
            }
        }
        assertNull(kvStore["key"])
    }

    @Test
    fun testNestedTransactionGetAll() {
        kvStore.inTransaction {
            set("key1", "value1")
            inTransaction {
                set("key2", "value2")
            }
        }
        assertEquals(mapOf("key1" to "value1", "key2" to "value2"), kvStore.getAll())
    }

    @Test
    fun testNestedTransactionRollbackOuter() {
        try {
            kvStore.inTransaction {
                set("key", "value1")
                inTransaction {
                    set("key", "value2")
                }
                throw Exception() // rollback outer transaction
            }
        } catch (e: Exception) {
            // expected
        }

        assertNull(kvStore["key"])
    }

    @Test
    fun `test Nested Transaction Rollback Inner`() {
        kvStore.inTransaction {
            set("key", "value1")
            try {
                inTransaction {
                    set("key", "value2")
                    throw Exception() // rollback inner transaction
                }
            } catch (e: Exception) {
                // expected
            }
        }
        assertEquals("value1", kvStore["key"])
    }


    @Test
    fun `test single transaction rollback`() {
        kvStore.inTransaction {
            set("key", "value")
            assertEquals("value", get("key"))
            rollbackTransaction()
        }
        assertNull(kvStore["key"])
    }

    @Test
    fun `test nested transactions rollback`() {
        kvStore.inTransaction {
            set("key1", "value1")
            inTransaction {
                set("key2", "value2")
                assertEquals("value2", get("key2"))
                rollbackTransaction()
            }
            assertEquals("value1", get("key1"))
            assertNull(kvStore["key2"])
            rollbackTransaction()
        }

        assertNull(kvStore["key1"])
        assertNull(kvStore["key2"])
    }

    @Test
    fun `test rollback of topmost transaction also rolls back all nested transactions`() {
        kvStore.inTransaction {
            set("key1", "value1")

            inTransaction {
                set("key2", "value2")

                assertEquals("value1", get("key1"))
                assertEquals("value2", get("key2"))

                rollbackTransaction()
            }

            assertEquals("value1", get("key1"))
            assertEquals(null, get("key2"))

            rollbackTransaction()
        }

        assertNull(kvStore.get("key1"))
        assertNull(kvStore.get("key2"))
    }
}