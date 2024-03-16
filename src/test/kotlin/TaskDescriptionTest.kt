import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import kotlin.test.BeforeTest
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class TaskDescriptionTest {
    private lateinit var kvStore: ITransactionalKeyValueStore

    private fun buildKeyValueStore() = TransactionalKeyValueStore(
        store = InMemoryKeyValueStore()
    )

    @BeforeTest
    fun setUp() {
        kvStore = buildKeyValueStore()
    }

    // Set and get a value:
    // > SET foo 123
    // > GET foo
    // 123

    @Test
    fun `Set and get a value`() {
        kvStore.set("foo", "123")
        assertEquals("123", kvStore.get("foo"))
    }

    // Delete a value
    // > SET foo 123
    // > DELETE foo
    // > GET foo
    // key not set

    @Test
    fun `Delete a value` () {
        kvStore.set("foo", "123")
        kvStore.delete("foo")
        assertNull(kvStore.get("key1"))
    }

    // Count the number of occurrences of a value
    // > SET foo 123
    // > SET bar 456
    // > SET baz 123
    // > COUNT 123
    // 2
    // > COUNT 456
    // 1

    @Test
    fun `Count the number of occurrences of a value `() {
        kvStore.set("foo", "123")
        kvStore.set("bar", "456")
        kvStore.set("baz", "123")
        assertEquals(2, kvStore.countValues("123"))
        assertEquals(1, kvStore.countValues("456"))
    }

    // Commit a transaction
    // > SET bar 123
    // > GET bar
    // 123
    // > BEGIN
    // > SET foo 456
    // > GET bar
    // 123
    // > DELETE bar
    // > COMMIT
    // > GET bar
    // key not set
    // > ROLLBACK
    // no transaction
    // > GET foo
    // 456

    @Test
    fun `Commit a transaction`() {
        kvStore.set("bar", "123")
        assertEquals("123", kvStore.get("bar"))
        kvStore.beginTransaction()
        kvStore.set("foo", "456")
        assertEquals("123", kvStore.get("bar"))
        kvStore.delete("bar")
        kvStore.commitTransaction()
        assertNull(kvStore.get("bar"))
        kvStore.rollbackTransaction()
        assertEquals("456", kvStore.get("foo"))
    }

    // Rollback a transaction
    // > SET foo 123
    // > SET bar abc
    // > BEGIN
    // > SET foo 456
    // > GET foo
    // 456
    // > SET bar def
    // > GET bar
    // def
    // > ROLLBACK
    // > GET foo
    // 123
    // > GET bar
    // abc
    // > COMMIT
    // no transaction

    @Test
    fun `Rollback a transaction`() {
        kvStore.set("foo", "123")
        kvStore.set("bar", "abc")
        kvStore.beginTransaction()
        kvStore.set("foo", "456")
        assertEquals("456", kvStore.get("foo"))
        kvStore.set("bar", "def")
        assertEquals("def", kvStore.get("bar"))
        kvStore.rollbackTransaction()
        assertEquals("123", kvStore.get("foo"))
        assertEquals("abc", kvStore.get("bar"))
        val hasTransaction = kvStore.commitTransaction()
        assertFalse(hasTransaction)
    }

    // Nested transactions
    // > SET foo 123
    // > SET bar 456
    // > BEGIN
    // > SET foo 456
    // > BEGIN
    // > COUNT 456
    // 2
    // > GET foo
    // 456
    // > SET foo 789
    // > GET foo
    // 789
    // > ROLLBACK
    // > GET foo
    // 456
    // > DELETE foo
    // > GET foo
    // key not set
    // > ROLLBACK
    // > GET foo
    // 123

    @Test
    fun `Nested transactions`() {
        kvStore.set("foo", "123")
        kvStore.set("bar", "456")
        kvStore.beginTransaction()
        kvStore.set("foo", "456")
        kvStore.beginTransaction()
        assertEquals(2, kvStore.countValues("456"))
        assertEquals("456", kvStore.get("foo"))
        kvStore.set("foo", "789")
        assertEquals("789", kvStore.get("foo"))
        kvStore.rollbackTransaction()
        assertEquals("456", kvStore.get("foo"))
        kvStore.delete("foo")
        assertNull(kvStore.get("foo"))
        kvStore.rollbackTransaction()
        assertEquals("123", kvStore.get("foo"))
    }
}