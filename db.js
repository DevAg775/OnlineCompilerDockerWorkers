import mongoose from 'mongoose'

const ExecutionSchema = new mongoose.Schema({
    executionId:  { type: String, required: true, unique: true },
    code:         { type: String, required: true },
    language:     { type: String, required: true },
    status:       { type: String, enum: ['pending', 'running', 'completed', 'failed'], default: 'pending' },
    output:       { type: String, default: null },
    exitCode:     { type: Number, default: null },
    error:        { type: String, default: null },
    createdAt:    { type: Date,   default: Date.now },
    completedAt:  { type: Date,   default: null }
})

export const Execution = mongoose.model('Execution', ExecutionSchema)

export async function connectMongo() {
    try {
        await mongoose.connect(process.env.MONGODB_URI || '')
        console.log('MongoDB connected')
    } catch (err) {
        console.error('MongoDB connection failed', err)
        process.exit(1)
    }
}