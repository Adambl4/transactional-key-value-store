import Commands.BEGIN
import Commands.COMMIT
import Commands.COUNT
import Commands.DELETE
import Commands.GET
import Commands.ROLLBACK
import Commands.SET
import org.junit.jupiter.api.Assertions.*
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFalse

class CLICommandsTest {
    private lateinit var kvStore: ITransactionalKeyValueStore

    private fun buildKeyValueStore() = TransactionalKeyValueStore(
        store = InMemoryKeyValueStore()
    )

    private class MockUserInteraction(val answer: Boolean = false) : UserInteraction {
        override fun askConfirmation(message: String) = answer
    }

    @BeforeTest
    fun setUp() {
        kvStore = buildKeyValueStore()
    }

    @Test
    fun testSetCommand() {
        val command = "$SET key value"
        processCommand(kvStore, command, UserInteractionNoOp)
        assertEquals("value", kvStore["key"])
    }

    @Test
    fun testDeleteCommand() {
        val setCommand = "$SET key value"
        processCommand(kvStore, setCommand, UserInteractionNoOp)
        assertEquals("value", kvStore["key"])

        val deleteCommand = "$DELETE key"
        val result = processCommand(kvStore, deleteCommand, MockUserInteraction(true))
        assertNull(result)
        assertNull(kvStore["key"])
    }

    @Test
    fun testBeginCommand() {
        assertFalse(kvStore.isInTransaction())
        processCommand(kvStore, BEGIN, UserInteractionNoOp)
        assertTrue(kvStore.isInTransaction())
    }
    @Test
    fun testCountCommand() {
        processCommand(kvStore, "$SET key1 value1", UserInteractionNoOp)
        processCommand(kvStore, "$SET key2 value1", UserInteractionNoOp)
        val result = processCommand(kvStore, "$COUNT value1", UserInteractionNoOp)
        assertEquals("2", result)
    }

    @Test
    fun testGetCommand() {
        processCommand(kvStore, "$SET key value", UserInteractionNoOp)
        val result = processCommand(kvStore, "$GET key", UserInteractionNoOp)
        assertEquals("value", result)
    }

    @Test
    fun testDeleteCommandWithConfirmation() {
        processCommand(kvStore, "$SET key value", UserInteractionNoOp)
        assertEquals("value", kvStore["key"])

        val result = processCommand(kvStore, "$DELETE key", MockUserInteraction(true))
        assertNull(result)
        assertNull(kvStore["key"])
    }

    @Test
    fun testDeleteCommandWithoutConfirmation() {
        processCommand(kvStore, "$SET key value", UserInteractionNoOp)
        assertEquals("value", kvStore["key"])
        val result = processCommand(kvStore, "$DELETE key", MockUserInteraction(false))
        assertEquals("Delete operation cancelled", result)
        assertEquals("value", kvStore["key"])
    }

    @Test
    fun testCommitCommandWithConfirmation() {
        processCommand(kvStore,  BEGIN, UserInteractionNoOp)
        assertTrue(kvStore.isInTransaction())

        processCommand(kvStore, "$SET key value", UserInteractionNoOp)
        assertEquals("value", kvStore["key"])

        val result = processCommand(kvStore, COMMIT, MockUserInteraction(true))
        assertNull(result)
        assertFalse(kvStore.isInTransaction())
        assertEquals("value", kvStore["key"])
    }

    @Test
    fun testCommitCommandWithoutConfirmation() {
        processCommand(kvStore, BEGIN, UserInteractionNoOp)
        assertTrue(kvStore.isInTransaction())

        processCommand(kvStore, "$SET key value", UserInteractionNoOp)
        assertEquals("value", kvStore["key"])

        val result = processCommand(kvStore, COMMIT, MockUserInteraction(false))
        assertEquals("Commit operation cancelled", result)
        assertTrue(kvStore.isInTransaction())
        assertEquals("value", kvStore["key"])
    }

    @Test
    fun testRollbackCommandWithConfirmation() {
        processCommand(kvStore, BEGIN, UserInteractionNoOp)
        assertTrue(kvStore.isInTransaction())

        processCommand(kvStore, "$SET key value", UserInteractionNoOp)
        assertEquals("value", kvStore["key"])

        val result = processCommand(kvStore, ROLLBACK, MockUserInteraction(true))
        assertNull(result)
        assertFalse(kvStore.isInTransaction())
        assertNull(kvStore["key"])
    }

    @Test
    fun testRollbackCommandWithoutConfirmation() {
        processCommand(kvStore, BEGIN, UserInteractionNoOp)
        assertTrue(kvStore.isInTransaction())

        processCommand(kvStore, "$SET key value", UserInteractionNoOp)
        assertEquals("value", kvStore["key"])

        val result = processCommand(kvStore, ROLLBACK, MockUserInteraction(false))
        assertEquals("Rollback operation cancelled", result)
        assertTrue(kvStore.isInTransaction())
        assertEquals("value", kvStore["key"])
    }
}

private object UserInteractionNoOp : UserInteraction {
    override fun askConfirmation(message: String): Boolean = error("Should not be invoked.")
}