package com.example.fastdm

import android.app.NotificationManager
import android.content.ContentValues
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Environment
import android.provider.MediaStore
import android.webkit.MimeTypeMap
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update
import okhttp3.*
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.net.URLDecoder
import java.util.Locale
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLongArray

enum class St { QUEUED, RUNNING, PAUSED, DONE, FAILED }

data class DItem(
    val id: String,
    val url: String,
    val name: String,
    val total: Long = -1,
    val done: Long = 0,
    val status: St = St.QUEUED,
    val speed: Long = 0,
    val segs: LongArray = LongArray(0),
    val uri: String? = null,
    val error: String? = null,
    val created: Long = System.currentTimeMillis()
)

private class Probe(val total: Long, val ranges: Boolean, val name: String?)

object Engine {
    private const val UA = "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/124 Mobile Safari/537.36"
    private lateinit var app: Context
    private val prefs get() = app.getSharedPreferences("fastdm", 0)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val jobs = ConcurrentHashMap<String, Job>()
    private val calls = ConcurrentHashMap<String, MutableSet<Call>>()
    val items = MutableStateFlow<List<DItem>>(emptyList())
    var maxSegs = 32
    var maxParallel = 3

    // HTTP/1.1 عشان كل جزء ياخد اتصال TCP لوحده
    private val client = OkHttpClient.Builder()
        .protocols(listOf(Protocol.HTTP_1_1))
        .connectionPool(ConnectionPool(64, 5, TimeUnit.MINUTES))
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .build()

    fun init(c: Context) {
        app = c.applicationContext
        maxSegs = prefs.getInt("segs", 32)
        maxParallel = prefs.getInt("par", 3)
        val a = JSONArray(prefs.getString("items", "[]")!!)
        items.value = (0 until a.length()).map {
            val o = a.getJSONObject(it)
            val sg = o.getJSONArray("segs")
            val st = St.valueOf(o.getString("st"))
            DItem(
                o.getString("id"), o.getString("url"), o.getString("name"),
                o.getLong("total"), o.getLong("done"),
                if (st == St.RUNNING || st == St.QUEUED) St.PAUSED else st,
                segs = LongArray(sg.length()) { i -> sg.getLong(i) },
                uri = o.optString("uri").ifEmpty { null },
                created = o.getLong("t")
            )
        }
    }

    private fun save() {
        val a = JSONArray()
        items.value.forEach {
            a.put(
                JSONObject().put("id", it.id).put("url", it.url).put("name", it.name)
                    .put("total", it.total).put("done", it.done).put("st", it.status.name)
                    .put("segs", JSONArray(it.segs.toList())).put("uri", it.uri ?: "")
                    .put("t", it.created)
            )
        }
        prefs.edit().putString("items", a.toString()).apply()
    }

    fun setPrefs(segs: Int, par: Int) {
        maxSegs = segs; maxParallel = par
        prefs.edit().putInt("segs", segs).putInt("par", par).apply()
        pump()
    }

    private fun set(id: String, f: (DItem) -> DItem) =
        items.update { l -> l.map { if (it.id == id) f(it) else it } }

    fun add(url: String) {
        val u = url.trim()
        if (!u.startsWith("http")) return
        val raw = u.substringBefore('?').substringAfterLast('/')
        val n = runCatching { URLDecoder.decode(raw, "UTF-8") }.getOrDefault(raw).ifBlank { "file" }
        items.update { it + DItem(UUID.randomUUID().toString(), u, n) }
        save(); pump()
    }

    @Synchronized
    private fun pump() {
        var running = items.value.count { it.status == St.RUNNING }
        for (d in items.value) {
            if (running >= maxParallel) break
            if (d.status == St.QUEUED) { start(d.id); running++ }
        }
    }

    fun pause(id: String) {
        set(id) { it.copy(status = St.PAUSED, speed = 0) }
        jobs[id]?.cancel()
        calls[id]?.forEach { it.cancel() }
        save(); pump()
    }

    fun resume(id: String) {
        set(id) { it.copy(status = St.QUEUED, error = null) }
        pump()
    }

    fun remove(id: String) {
        pause(id)
        items.update { l -> l.filter { it.id != id } }
        File(app.getExternalFilesDir(null), "$id.part").delete()
        save()
    }

    private fun start(id: String) {
        set(id) { it.copy(status = St.RUNNING, error = null) }
        ContextCompat.startForegroundService(app, Intent(app, DownloadService::class.java))
        jobs[id] = scope.launch {
            try {
                work(id)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                set(id) { it.copy(status = St.FAILED, speed = 0, error = e.message) }
            } finally {
                jobs.remove(id, coroutineContext.job)
                save(); pump()
            }
        }
    }

    private suspend fun work(id: String) {
        coroutineScope {
            val d = items.value.first { it.id == id }
            val part = File(app.getExternalFilesDir(null), "$id.part")
            val p = probe(d.url)
            val total = p.total
            val resumable = p.ranges && d.total == total && d.segs.isNotEmpty() &&
                part.exists() && part.length() == total
            val n = when {
                resumable -> d.segs.size
                p.ranges && total >= 1_000_000 -> minOf(maxSegs, maxOf(1, (total / 400_000).toInt()))
                else -> 1
            }
            val start0 = if (resumable) d.segs else LongArray(n)
            if (!resumable) RandomAccessFile(part, "rw").use { it.setLength(maxOf(total, 0L)) }
            set(id) { it.copy(total = total, segs = start0, done = start0.sum(), name = p.name ?: it.name) }

            val ranged = p.ranges && total > 0
            val size = if (n > 1) (total + n - 1) / n else total
            val prog = AtomicLongArray(start0)

            val ticker = launch {
                var last = (0 until n).sumOf { prog[it] }
                var t0 = System.nanoTime()
                var k = 0
                while (true) {
                    delay(500)
                    val now = (0 until n).sumOf { prog[it] }
                    val t1 = System.nanoTime()
                    val sp = (now - last) * 1_000_000_000L / maxOf(1L, t1 - t0)
                    last = now; t0 = t1
                    set(id) { it.copy(done = now, speed = sp, segs = LongArray(n) { i -> prog[i] }) }
                    if (++k % 6 == 0) save()
                }
            }
            try {
                (0 until n).map { i ->
                    val end = if (ranged) minOf(total, (i + 1) * size) - 1 else Long.MAX_VALUE
                    launch { seg(id, d.url, i, i * size, end, prog, part, ranged) }
                }.joinAll()
            } finally {
                ticker.cancel()
            }

            val sum = (0 until n).sumOf { prog[it] }
            if (total > 0 && sum < total) throw IOException("التحميل ناقص، جرّب استكمال")
            val cur = items.value.first { it.id == id }
            val uri = publish(part, cur.name)
            part.delete()
            set(id) { it.copy(status = St.DONE, speed = 0, done = sum, total = sum, uri = uri.toString()) }
            save()
            val note = NotificationCompat.Builder(app, "dl")
                .setSmallIcon(android.R.drawable.stat_sys_download_done)
                .setContentTitle("تم التحميل").setContentText(cur.name).setAutoCancel(true).build()
            app.getSystemService(NotificationManager::class.java).notify(id.hashCode(), note)
        }
    }

    private suspend fun seg(
        id: String, url: String, i: Int, start: Long, end: Long,
        prog: AtomicLongArray, file: File, ranged: Boolean
    ) {
        var tries = 0
        while (true) {
            currentCoroutineContext().ensureActive()
            if (!ranged) prog.set(i, 0L)
            val from = start + prog[i]
            if (ranged && from > end) return
            val rb = Request.Builder().url(url).header("User-Agent", UA)
            if (ranged) rb.header("Range", "bytes=$from-$end")
            val call = client.newCall(rb.build())
            val cs = calls.getOrPut(id) { ConcurrentHashMap.newKeySet() }
            cs.add(call)
            try {
                call.execute().use { r ->
                    if (!r.isSuccessful) throw IOException("HTTP ${r.code}")
                    if (ranged && r.code != 206) throw IOException("الخادم تجاهل Range")
                    val src = r.body!!.byteStream()
                    RandomAccessFile(file, "rw").use { raf ->
                        raf.seek(from)
                        val buf = ByteArray(256 * 1024)
                        while (true) {
                            val got = src.read(buf)
                            if (got < 0) break
                            raf.write(buf, 0, got)
                            prog.addAndGet(i, got.toLong())
                            if (ranged && start + prog[i] > end) break
                        }
                    }
                }
                return
            } catch (e: IOException) {
                currentCoroutineContext().ensureActive()
                if (++tries > 5) throw e
                delay(1000L * tries)
            } finally {
                cs.remove(call)
            }
        }
    }

    private fun probe(url: String): Probe {
        val req = Request.Builder().url(url).header("User-Agent", UA).header("Range", "bytes=0-0").build()
        client.newCall(req).execute().use { r ->
            if (!r.isSuccessful) throw IOException("HTTP ${r.code}")
            val cd = r.header("Content-Disposition")
            val name = cd?.let { c ->
                val star = c.substringAfter("filename*=UTF-8''", "")
                val plain = c.substringAfter("filename=", "")
                star.ifEmpty { plain }.substringBefore(';').trim().trim('"')
            }?.takeIf { it.isNotBlank() }
                ?.let { runCatching { URLDecoder.decode(it, "UTF-8") }.getOrDefault(it) }
                ?.replace('/', '_')
            val cr = r.header("Content-Range")
            return if (r.code == 206 && cr != null)
                Probe(cr.substringAfter('/').toLongOrNull() ?: -1, true, name)
            else
                Probe(r.body?.contentLength() ?: -1, false, name)
        }
    }

    private fun publish(src: File, name: String): Uri {
        val cv = ContentValues().apply {
            put(MediaStore.Downloads.DISPLAY_NAME, name)
            put(MediaStore.Downloads.MIME_TYPE, mime(name))
            put(MediaStore.Downloads.RELATIVE_PATH, Environment.DIRECTORY_DOWNLOADS + "/FastDM")
            put(MediaStore.Downloads.IS_PENDING, 1)
        }
        val r = app.contentResolver
        val uri = r.insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, cv)
            ?: throw IOException("مقدرتش أنشئ الملف")
        r.openOutputStream(uri)!!.use { o -> src.inputStream().use { it.copyTo(o, 1 shl 20) } }
        cv.clear()
        cv.put(MediaStore.Downloads.IS_PENDING, 0)
        r.update(uri, cv, null, null)
        return uri
    }

    fun mime(n: String): String =
        MimeTypeMap.getSingleton().getMimeTypeFromExtension(n.substringAfterLast('.', "").lowercase())
            ?: "application/octet-stream"

    fun cat(n: String): String = when (n.substringAfterLast('.', "").lowercase()) {
        "mp4", "mkv", "avi", "mov", "webm", "3gp" -> "فيديو"
        "mp3", "m4a", "flac", "wav", "ogg", "aac" -> "صوت"
        "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "epub" -> "مستندات"
        "zip", "rar", "7z", "tar", "gz" -> "مضغوطات"
        "apk", "exe", "msi", "iso" -> "برامج"
        "jpg", "jpeg", "png", "gif", "webp" -> "صور"
        else -> "أخرى"
    }
}

fun fmt(b: Long): String {
    var v = b.toDouble()
    var i = 0
    val u = arrayOf("B", "KB", "MB", "GB")
    while (v >= 1024 && i < 3) { v /= 1024; i++ }
    return String.format(Locale.US, "%.1f %s", v, u[i])
}

fun dur(s: Long): String = when {
    s < 0 -> "—"
    s < 60 -> "$s ث"
    s < 3600 -> "${s / 60} د"
    else -> "${s / 3600} س ${(s % 3600) / 60} د"
}
