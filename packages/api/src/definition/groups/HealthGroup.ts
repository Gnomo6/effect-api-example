import { HttpApiEndpoint, HttpApiGroup } from '@effect/platform'
import { Schema as S } from 'effect'

export const HealthGroup = HttpApiGroup.make('health').add(
    HttpApiEndpoint.get('healthCheck', '/health').addSuccess(
        S.Struct({
            status: S.Literal('ok'),
            timestamp: S.DateTimeUtc,
            version: S.String,
            environment: S.String,
        }),
    ),
)

