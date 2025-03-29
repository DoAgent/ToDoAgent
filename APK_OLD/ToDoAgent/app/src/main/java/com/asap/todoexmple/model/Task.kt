data class Task(
    val listId: String,
    val userId: String,
    val startTime: String?,
    val endTime: String?,
    val location: String?,
    val content: String,
    var isImportant: Boolean = false,
    var isCompleted: Boolean = false,
    val syncStatus: Int,
    val lastModified: String?
) {
    fun copy(
        isImportant: Boolean = this.isImportant,
        isCompleted: Boolean = this.isCompleted
    ): Task = Task(
        listId = this.listId,
        userId = this.userId,
        startTime = this.startTime,
        endTime = this.endTime,
        location = this.location,
        content = this.content,
        isImportant = isImportant,
        isCompleted = isCompleted,
        syncStatus = this.syncStatus,
        lastModified = this.lastModified
    )
} 