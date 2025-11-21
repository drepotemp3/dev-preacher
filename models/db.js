import mongoose from "mongoose";

// Account Schema
const accountSchema = new mongoose.Schema(
  {
    number: { type: String, required: true, unique: true },
    username: String,
    admin: { type: Boolean, default: false },
    session: String,
    currentMessageId: { type: Number, default: 0 }, // Account-level message ID
    groups: [
      {
        dailyTracker: [{ date: String, messageCount: Number, lastSentAt:String }],
        name: String,
        link: String,
        msgPerDay: { type: Number, default: 5 },
        id: String,
        lastMessageId: { type: Number, default: 0 }, // Keep for backward compatibility
      },
    ],
  },
  { timestamps: true }
);

export const Account = mongoose.model("Account", accountSchema);

// Customer Schema - For storing DM/reply interactions
const customerSchema = new mongoose.Schema(
  {
    username: String,
    userId: String,
    textedAt: { type: Date, default: Date.now },
    type: { type: String, enum: ['dm', 'reply'], required: true },
    content: String,
    senderAccount: String, // Account that received the DM/reply
    groupId: String, // For replies, the group where the reply was made
  },
  { timestamps: true }
);

export const Customer = mongoose.model("Customer", customerSchema);

// System Schema - For bot settings
const systemSchema = new mongoose.Schema(
  {
    reportChannel: { type: String, default: null },
  },
  { timestamps: true }
);

export const System = mongoose.model("System", systemSchema);

// Connect to MongoDB
export async function connectDB() {
  try {
    await mongoose.connect(process.env.MONGODB_URI, {
      dbName: "dev-preacher",
    });

    console.log("✅ Connected to MongoDB");
    
    
  } catch (error) {
    console.error("❌ MongoDB connection error:", error);
    throw error;
  }
}
