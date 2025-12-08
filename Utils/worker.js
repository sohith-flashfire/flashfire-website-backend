import { Worker } from 'bullmq';
import Twilio from 'twilio';
import dotenv from 'dotenv';
import { sendWhatsAppMessage } from '../Utils/WatiHelper.js';
import { Logger } from './Logger.js';
import { CampaignBookingModel } from '../Schema_Models/CampaignBooking.js';
import { redisConnection } from './queue.js';

dotenv.config();

console.log('\n📞 ========================================');
console.log('📞 [CallWorker] Initializing Call Reminder Worker');
console.log('📞 ========================================\n');

console.log('🔄 [CallWorker] Using shared ioredis connection from queue.js');

const client = Twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);

const worker = new Worker(
  'callQueue',
  async (job) => {
    const { type } = job.data;

    console.log('\n📥 ========================================');
    console.log(`📥 [CallWorker] Job Received: ${job.id}`);
    console.log('📥 ========================================');
    console.log(`📌 Job Type: ${type || 'call_reminder'}`);
    console.log(`📌 Job Data:`, JSON.stringify(job.data, null, 2));
    console.log('========================================\n');

    try {
      if (type === 'payment_reminder') {
        console.log('💰 [CallWorker] Processing payment reminder...');
        await processPaymentReminder(job);
      } else {
        console.log('📞 [CallWorker] Processing call reminder...');
        await processCallReminder(job);
      }
    } catch (err) {
      console.error(`❌ [CallWorker] Job ${job.id} processing failed:`, err.message);
      Logger.error(`Job processing failed: ${job.id}`, {
        error: err.message,
        stack: err.stack,
        jobData: job.data
      });
      throw err; // important so BullMQ marks job as failed
    }
  },
  { connection: redisConnection }
);

// Process payment reminder jobs
async function processPaymentReminder(job) {
  const { bookingId, clientName, clientPhone, paymentLink, reminderDays } = job.data;
  
  Logger.info(`Processing payment reminder job for ${clientName}`, {
    jobId: job.id,
    bookingId,
    clientPhone,
    reminderDays
  });

  const message = `Hello ${clientName},

I hope this message finds you well. I wanted to reach out regarding the payment information we discussed during our consultation.

As mentioned, here are the payment details for our services:

Payment Link: ${paymentLink}

Please feel free to review the payment options at your convenience. If you have any questions about the pricing, payment methods, or need to discuss a payment plan, I'm here to help.

You can also visit our website at https://www.flashfirejobs.com/ for more information about our services.

Thank you for considering FlashFire for your career development needs. I look forward to hearing from you soon.

Best regards,
FlashFire Team`;

  const result = await sendWhatsAppMessage(clientPhone, message);
  
  if (result.success) {
    // Update payment reminder status in database
    await CampaignBookingModel.findOneAndUpdate(
      { 
        bookingId,
        'paymentReminders.jobId': job.id.toString()
      },
      { 
        $set: { 
          'paymentReminders.$.status': 'sent',
          'paymentReminders.$.sentAt': new Date()
        }
      }
    );

    Logger.info(`Payment reminder sent successfully to ${clientName}`, {
      jobId: job.id,
      bookingId,
      clientPhone
    });
  } else {
    // Update payment reminder status to failed
    await CampaignBookingModel.findOneAndUpdate(
      { 
        bookingId,
        'paymentReminders.jobId': job.id.toString()
      },
      { 
        $set: { 
          'paymentReminders.$.status': 'failed'
        }
      }
    );

    Logger.error(`Failed to send payment reminder to ${clientName}`, {
      jobId: job.id,
      bookingId,
      clientPhone,
      error: result.error
    });
    throw new Error(`WhatsApp message failed: ${result.error}`);
  }
}

// Process call reminder jobs (existing functionality)
async function processCallReminder(job) {
  console.log('\n🔍 [CallWorker] Starting call reminder processing...');
  
  const meta = {
    jobId: job?.id,
    type: job?.data?.type || 'call_reminder',
    phone: job?.data?.phone,
    meetingTime: job?.data?.meetingTime,
    inviteeEmail: job?.data?.inviteeEmail
  };
  
  console.log('📋 [CallWorker] Job Details:');
  console.log('   • Job ID:', meta.jobId);
  console.log('   • Phone:', meta.phone);
  console.log('   • Meeting Time:', meta.meetingTime);
  console.log('   • Invitee Email:', meta.inviteeEmail);

  const phone = job?.data?.phone;
  if (!phone) {
    console.error('❌ [CallWorker] Missing phone number - aborting call');
    Logger.error('[Worker] Missing phone in job data; aborting call', meta);
    return;
  }

  console.log('✅ [CallWorker] Phone number found:', phone);

  const phoneRegex = /^\+?[1-9]\d{9,14}$/;
  if (!phoneRegex.test(phone)) {
    console.error('❌ [CallWorker] Invalid phone format (must be E.164):', phone);
    Logger.error('[Worker] Invalid E.164 phone format; aborting call', { ...meta, phone });
    return;
  }

  console.log('✅ [CallWorker] Phone format validated (E.164)');

  if (!process.env.TWILIO_FROM) {
    console.error('❌ [CallWorker] TWILIO_FROM not configured - aborting call');
    Logger.error('[Worker] TWILIO_FROM not configured; aborting call');
    return;
  }

  console.log('✅ [CallWorker] Twilio FROM number configured:', process.env.TWILIO_FROM);

  try {
    console.log('\n📞 [CallWorker] Initiating Twilio call...');
    console.log('   → To:', phone);
    console.log('   → From:', process.env.TWILIO_FROM);
    console.log('   → Meeting Time:', job.data.meetingTime);
    
    const call = await client.calls.create({
      to: phone,
      from: process.env.TWILIO_FROM,
      url: `https://api.flashfirejobs.com/twilio-ivr?meetingTime=${encodeURIComponent(job.data.meetingTime)}`
    });

    console.log('\n✅ [CallWorker] Call initiated successfully!');
    console.log('   • Call SID:', call?.sid);
    console.log('   • Status:', call?.status);
    console.log('   • To:', phone);
    console.log('========================================\n');
  } catch (error) {
    console.error('\n❌ [CallWorker] Twilio call FAILED!');
    console.error('   • Job ID:', job?.id);
    console.error('   • Phone:', phone);
    console.error('   • Error:', error?.message);
    console.error('   • Code:', error?.code);
    console.error('   • More Info:', error?.moreInfo);
    console.error('========================================\n');
    
    Logger.error('[Worker] Twilio call failed', {
      jobId: job?.id,
      phone,
      error: error?.message,
      code: error?.code,
      moreInfo: error?.moreInfo
    });
    throw error;
  }
}

// Track worker lifecycle with detailed logs
console.log('✅ [CallWorker] Worker connected to Redis successfully!');
console.log('👂 [CallWorker] Listening for jobs on "callQueue"...\n');

worker.on("completed", (job) => {
  console.log('\n🎉 ========================================');
  console.log(`🎉 [CallWorker] Job Completed: ${job.id}`);
  console.log('🎉 ========================================\n');
});

worker.on("failed", (job, err) => {
  console.error('\n💥 ========================================');
  console.error(`💥 [CallWorker] Job Failed: ${job?.id}`);
  console.error('💥 Error:', err.message);
  console.error('💥 ========================================\n');
});

worker.on("error", (err) => {
  console.error('\n⚠️  [CallWorker] Worker error:', err.message);
});

worker.on("ioredis:close", () => {
  console.warn('\n⚠️  [CallWorker] Redis connection closed!');
});

worker.on("ioredis:reconnecting", () => {
  console.log('\n🔄 [CallWorker] Reconnecting to Redis...');
});
