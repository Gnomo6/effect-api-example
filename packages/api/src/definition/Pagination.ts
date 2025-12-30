import { Schema as S } from 'effect'

const DEFAULT_LIMIT = 25 as const
const DEFAULT_MAX_QUERY = 100 as const
const DEFAULT_MIN_QUERY = 1 as const

export const ApiResponse = <A extends { id: string }, I, R>(dataSchema: S.Schema<A, I, R>) =>
    S.Struct({
        hasMore: S.Boolean,
        data: S.Array(dataSchema),
    })

export const ApiQueryParams = <A extends string, I, R>(
    idSchema: S.Schema<A, I, R>,
    limit: {
        default: number
        max: number
        min: number
    } = {
        default: DEFAULT_LIMIT,
        max: DEFAULT_MAX_QUERY,
        min: DEFAULT_MIN_QUERY,
    },
) =>
    S.Struct({
        limit: S.optional(
            S.NumberFromString.pipe(S.int(), S.greaterThanOrEqualTo(limit.min), S.lessThanOrEqualTo(limit.max)),
        ).pipe(S.withDecodingDefault(() => limit.default)),
        afterId: S.optional(idSchema),
        beforeId: S.optional(idSchema),
    })

