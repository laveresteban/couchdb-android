package org.couchdb.sync.android

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.work.WorkInfo
import androidx.work.WorkManager
import androidx.work.testing.WorkManagerTestInitHelper
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [34])
class SyncSchedulerTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()

    @Before
    fun initWorkManager() {
        WorkManagerTestInitHelper.initializeTestWorkManager(context)
    }

    private fun jobs(db: String) = WorkManager.getInstance(context).getWorkInfosByTag("couchlite:$db").get()

    @Test
    fun `periodic sync is scheduled once per database`() {
        SyncScheduler.schedulePeriodic(context, "notes")
        SyncScheduler.schedulePeriodic(context, "notes")
        assertEquals(1, jobs("notes").size)
    }

    @Test
    fun `a sync now request shows up in the status flow`() =
        runBlocking<Unit> {
            SyncScheduler.syncNow(context, "notes")
            val status = withTimeout(10_000) { SyncScheduler.status(context, "notes").first { it.isNotEmpty() } }
            assertEquals(WorkInfo.State.ENQUEUED, status.single().state)
        }

    @Test
    fun `cancel stops the jobs for a database`() {
        SyncScheduler.schedulePeriodic(context, "cancel-me")
        SyncScheduler.syncNow(context, "cancel-me")
        SyncScheduler.cancel(context, "cancel-me")
        assertTrue(jobs("cancel-me").all { it.state == WorkInfo.State.CANCELLED })
    }
}
