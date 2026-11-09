package com.example.fastdm

import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.IBinder
import androidx.core.app.NotificationCompat
import kotlinx.coroutines.*

class DownloadService : Service() {
    private var job: Job? = null

    override fun onBind(i: Intent?): IBinder? = null

    private fun note(text: String) = NotificationCompat.Builder(this, "dl")
        .setSmallIcon(android.R.drawable.stat_sys_download)
        .setContentTitle("FastDM").setContentText(text).setOngoing(true).build()

    override fun onStartCommand(i: Intent?, f: Int, startId: Int): Int {
        startForeground(1, note("جاري التحميل..."), ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
        job?.cancel()
        job = CoroutineScope(Dispatchers.Default).launch {
            Engine.items.collect { list ->
                val act = list.filter { it.status == St.RUNNING }
                if (act.isEmpty() && list.none { it.status == St.QUEUED }) {
                    stopSelf()
                    return@collect
                }
                getSystemService(NotificationManager::class.java)
                    .notify(1, note("${act.size} تحميل • ${fmt(act.sumOf { it.speed })}/ث"))
            }
        }
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        job?.cancel()
    }
}
