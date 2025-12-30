import { HttpApiError, HttpApiMiddleware, HttpApiSchema, HttpApiSecurity } from '@effect/platform'
import { Context, Schema as S } from 'effect'

export class ApiKeyUnauthorized extends S.TaggedError<ApiKeyUnauthorized>()(
    'ApiKeyUnauthorized',
    {
        message: S.String,
    },
    HttpApiSchema.annotations({ status: 401 }),
) {}

export class ApiKeyAuthContext extends Context.Tag('ApiKeyAuthContext')<
    ApiKeyAuthContext,
    {
        apiKeyId: string
    }
>() {}

export class ApiKeyAuthMiddleware extends HttpApiMiddleware.Tag<ApiKeyAuthMiddleware>()(
    'PublicApiKeyAuthMiddleware',
    {
        failure: S.Union(ApiKeyUnauthorized, HttpApiError.InternalServerError),
        provides: ApiKeyAuthContext,
        security: {
            apiKey: HttpApiSecurity.apiKey({
                key: 'x-api-key',
                in: 'header',
            }),
        },
    },
) {}
