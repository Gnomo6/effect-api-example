import { HttpApiBuilder } from '@effect/platform'
import { WarpApi } from '@effect-api-example/api/definition'
import { DateTime, Effect } from 'effect'

export const HealthGroupLive = HttpApiBuilder.group(WarpApi, 'health', (handlers) =>
    Effect.gen(function* () {
        return handlers.handle('healthCheck', () => {
            return Effect.succeed({
                status: 'ok' as const,
                timestamp: DateTime.unsafeNow(),
                version: '0.0.1',
                environment: process.env.NODE_ENV || 'development',
            })
        })
    }),
)
