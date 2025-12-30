import { ApiKeyAuthMiddleware, ApiKeyUnauthorized } from '@effect-api-example/api/definition'
import { Effect, Layer, Redacted } from 'effect'

export const ApiKeyAuthMiddlewareLive = Layer.effect(
    ApiKeyAuthMiddleware,
    Effect.gen(function* () {
        return {
            apiKey: (apiKey) =>
                Effect.gen(function* () {
                    const apiKeyValue = Redacted.value(apiKey)

                    if (!apiKeyValue) {
                        yield* Effect.logWarning('No API key provided in x-api-key header')
                        return yield* Effect.fail(
                            new ApiKeyUnauthorized({ message: 'API key is required' }),
                        )
                    }

                    //
                    // some actual api key validation would go here
                    // 

                    
                    return {
                        apiKeyId: apiKeyValue,
                    }
                }),
        }
    }),
)
