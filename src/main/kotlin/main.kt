import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.collections.LinkedHashMap
import kotlin.concurrent.read
import kotlin.concurrent.write

fun main() {
    val scanner = Scanner(System.`in`)

    runFromCliInput(
        kvStore = TransactionalKeyValueStore(
            store = InMemoryKeyValueStore()
        ),
        scanner = scanner,
        userInteraction = UserInteraction.Default(scanner)
    )
}

private fun runFromCliInput(
    kvStore: ITransactionalKeyValueStore,
    scanner: Scanner,
    userInteraction: UserInteraction
) {
    while (true) {
        print("> ")
        val input = scanner.nextLine()

        try {
            val result = processCommand(
                kvStore = kvStore,
                command = input,
                userInteraction = userInteraction
            )
            if (result != null) {
                println(result)
            }
        } catch (e: Exception) {
            println("Error processing command: ${e.message}")
        }
    }
}

internal fun processCommand(
    kvStore: ITransactionalKeyValueStore,
    command: String,
    userInteraction: UserInteraction
): String? {
    try {
        val parts = command.trim().split(" ")

        when (parts[0].uppercase()) {
            Commands.SET -> {
                check(parts.size == 3) { "${Commands.SET} command must have 2 arguments" }

                kvStore[parts[1]] = parts[2]
            }

            Commands.DELETE -> {
                check(parts.size == 2) { "${Commands.DELETE} command must have 1 argument" }

                if (userInteraction.askConfirmation("Are you sure you want to delete?")) {
                    kvStore.delete(parts[1])
                } else {
                    return "Delete operation cancelled"
                }
            }

            Commands.COUNT -> {
                check(parts.size == 2) { "${Commands.COUNT} command must have 1 argument" }

                return kvStore.countValues(parts[1]).toString()
            }

            Commands.GET -> {
                check(parts.size == 2) { "${Commands.GET} command must have 1 argument" }

                return kvStore[parts[1]] ?: "key not set"
            }

            Commands.BEGIN -> {
                check(parts.size == 1) { "${Commands.BEGIN} command must have 0 arguments" }

                kvStore.beginTransaction()
            }

            Commands.COMMIT -> {
                check(parts.size == 1) { "${Commands.COMMIT} command must have 0 arguments" }

                if (userInteraction.askConfirmation("Are you sure you want to commit?")) {
                    if (!kvStore.commitTransaction()) return "no transaction"
                } else {
                    return "Commit operation cancelled"
                }
            }

            Commands.ROLLBACK -> {
                check(parts.size == 1) { "${Commands.ROLLBACK} command must have 0 arguments" }

                if (userInteraction.askConfirmation("Are you sure you want to rollback?")) {
                    if (!kvStore.rollbackTransaction()) return "no transaction"
                } else {
                    return "Rollback operation cancelled"
                }
            }

            else -> throw RuntimeException("Unknown command")
        }
    } catch (e: Exception) {
        return "Error processing operation: ${e.message}"
    }

    return null
}

object Commands {
    const val SET = "SET"
    const val DELETE = "DELETE"
    const val BEGIN = "BEGIN"
    const val COMMIT = "COMMIT"
    const val ROLLBACK = "ROLLBACK"
    const val COUNT = "COUNT"
    const val GET = "GET"
}

interface UserInteraction {
    fun askConfirmation(message: String): Boolean

    companion object {
        fun Default(scanner: Scanner): UserInteraction = object : UserInteraction {
            override fun askConfirmation(message: String): Boolean {
                print("$message. y/n > ")
                val input = scanner.nextLine()
                return input.equals("y", ignoreCase = true)
            }
        }
    }
}

interface IKeyValueStore {
    operator fun set(key: String, value: String)
    operator fun get(key: String): String?
    fun delete(key: String)
    fun getAll(): Map<String, String>
}

fun IKeyValueStore.vales() = getAll().values
fun IKeyValueStore.keys() = getAll().keys
fun IKeyValueStore.size() = getAll().size
fun IKeyValueStore.countValues(value: String) = vales().count { it == value }
fun IKeyValueStore.isEmpty() = getAll().isEmpty()

internal class InMemoryKeyValueStore(
    private val data: MutableMap<String, String>
) : IKeyValueStore {
    override fun set(key: String, value: String) {
        data[key] = value
    }

    override fun get(key: String): String? = data[key]

    override fun delete(key: String) {
        data.remove(key)
    }

    override fun getAll(): Map<String, String> = data
}

fun InMemoryConcurrentKeyValueStore(
    defaultData: Map<String, String> = emptyMap()
): IKeyValueStore = InMemoryKeyValueStore(data = ConcurrentHashMap(defaultData))

fun InMemoryConcurrentKeyValueStore(
    initialCapacity: Int
): IKeyValueStore = InMemoryKeyValueStore(data = ConcurrentHashMap(initialCapacity))

fun InMemoryKeyValueStore(
    defaultData: Map<String, String> = emptyMap()
): IKeyValueStore = InMemoryKeyValueStore(data = HashMap(defaultData))

fun InMemoryKeyValueStore(
    initialCapacity: Int
): IKeyValueStore = InMemoryKeyValueStore(data = HashMap(initialCapacity))

interface ITransactionalKeyValueStore : IKeyValueStore {
    fun beginTransaction()
    fun commitTransaction(): Boolean
    fun rollbackTransaction(): Boolean
    fun isInTransaction(): Boolean
    fun inTransaction(block: ITransactionalKeyValueStore.() -> Unit)
}

class TransactionalKeyValueStore(
    private val store: IKeyValueStore
) : ITransactionalKeyValueStore {
    private val transactions = ThreadLocal.withInitial { ArrayDeque<Transaction>() }
    private val lock = ReentrantReadWriteLock()

    override fun set(key: String, value: String) = lock.write {
        val currentTransaction = currentTransaction()

        if (currentTransaction != null) {
            currentTransaction.context[key] = value
        } else {
            store[key] = value
        }
    }

    override fun get(key: String): String? = lock.read {
        var transaction = currentTransaction()

        while (transaction != null) {
            if (transaction.context.containsKey(key)) {
                return transaction.context[key]
            }
            transaction = transaction.parent
        }

        return store[key]
    }

    override fun delete(key: String) = lock.write {
        val currentTransaction = currentTransaction()

        if (currentTransaction != null) {
            currentTransaction.context[key] = null
        } else {
            store.delete(key)
        }
    }

    override fun getAll(): Map<String, String> = lock.read {
        var transaction = currentTransaction()

        val allData = if (transaction != null) {
            // lazy copy of map
            LinkedHashMap(store.getAll())
        } else {
            store.getAll() as MutableMap<String, String>
        }

        while (transaction != null) {
            transaction.context.forEach { (key, value) ->
                if (value == null) {
                    allData.remove(key)
                } else {
                    allData[key] = value
                }
            }
            transaction = transaction.parent
        }

        return allData
    }

    override fun beginTransaction() {
        beginTransactionInternal()
    }

    override fun commitTransaction() = lock.write {
        if (!isInTransaction()) return@write false

        val transaction = transactions.get().pop()

        transaction.context.forEach { (key, value) ->
            if (transaction.parent != null) {
                // if this is a nested transaction, merge its changes into the parent transaction
                if (value == null) {
                    transaction.parent.context.remove(key)
                } else {
                    transaction.parent.context[key] = value
                }
            } else {
                // ff this is the outermost transaction, apply its changes to the underlying store
                if (value == null) {
                    store.delete(key)
                } else {
                    store[key] = value
                }
            }
        }

        return@write true
    }

    override fun rollbackTransaction() = lock.write {
        if (!isInTransaction()) return@write false

        transactions.get().pop()

        return@write true
    }

    override fun isInTransaction(): Boolean {
        return currentTransaction() != null
    }

    override fun inTransaction(block: ITransactionalKeyValueStore.() -> Unit) = lock.write {
        val transaction = beginTransactionInternal()
        try {
            block()
            if (currentTransaction() == transaction) {
                commitTransaction()
            }
        } catch (e: Throwable) {
            if (currentTransaction() == transaction) {
                rollbackTransaction()
            }
            throw e
        }
    }

    private fun currentTransaction(): Transaction? {
        return transactions.get().peek()
    }

    private fun beginTransactionInternal() = lock.write {
        val parentTransaction = currentTransaction()

        if (parentTransaction != null && parentTransaction.context.isEmpty()) {
            // avoid nested empty transactions
            return@write parentTransaction
        }

        val transaction = Transaction(
            parent = parentTransaction
        )

        transactions.get().push(transaction)

        return@write transaction
    }

    private data class Transaction(
        val parent: Transaction?
    ) {
        val context = LinkedHashMap<String, String?>()
    }
}