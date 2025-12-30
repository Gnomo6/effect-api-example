import { HttpApiBuilder, HttpApiError } from '@effect/platform'
import { WarpApi, PublicEmployeeSchema, EmployeeNotFoundError, PublicEmployee } from '@effect-api-example/api/definition'
import { EmployeeTag } from '@effect-api-example/shared'
import * as PgDrizzle from '@effect/sql-drizzle/Pg'
import { employees } from '../../db/schema/employees'
import { and, asc, eq, gt, inArray, lt, SQL } from 'drizzle-orm'
import { DateTime, Effect } from 'effect'

export const EmployeesGroupLive = HttpApiBuilder.group(WarpApi, 'Employees', (handlers) =>
    Effect.gen(function* () {
        return handlers
            .handle('list', ({ urlParams: { types, limit, afterId, beforeId } }) =>
                Effect.gen(function* () {
                    const drizzle = yield* PgDrizzle.PgDrizzle

                    const filters: SQL[] = []

                    if (types && types.length > 0) {
                        filters.push(inArray(employees.type, types))
                    }

                    if (afterId) {
                        filters.push(gt(employees.tag, afterId))
                    }

                    if (beforeId) {
                        filters.push(lt(employees.tag, beforeId))
                    }

                    const results = yield* drizzle
                        .select()
                        .from(employees)
                        .where(filters.length > 0 ? and(...filters) : undefined)
                        .orderBy(asc(employees.tag))
                        .limit(limit + 1)
                        .pipe(
                            Effect.catchAll((e) =>
                                Effect.gen(function* () {
                                    yield* Effect.logError('Failed to list employees: ', e)
                                    return yield* Effect.fail(new HttpApiError.InternalServerError())
                                }),
                            ),
                        )

                    const hasMore = results.length > limit
                    const pageData = results.slice(0, limit)

                    return {
                        hasMore,
                        data: pageData.map(dbEmployeeToPublicEmployee),
                    }
                }),
            )
            .handle('get', ({ path: { id } }) =>
                Effect.gen(function* () {
                    const db = yield* PgDrizzle.PgDrizzle

                    const results = yield* db
                        .select()
                        .from(employees)
                        .where(eq(employees.tag, id))
                        .limit(1)
                        .pipe(
                            Effect.catchAll((e) =>
                                Effect.gen(function* () {
                                    yield* Effect.logError('Failed to get employee: ', e)
                                    return yield* Effect.fail(new HttpApiError.InternalServerError())
                                }),
                            ),
                        )

                    if (results.length === 0) {
                        return yield* Effect.fail(
                            new EmployeeNotFoundError({
                                id,
                                message: `Employee not found: ${id}`,
                            }),
                        )
                    }

                    return dbEmployeeToPublicEmployee(results[0])
                }),
            )
    }),
)

type DbEmployee = typeof employees.$inferSelect

function dbEmployeeToPublicEmployee(emp: DbEmployee): PublicEmployee {
    return PublicEmployeeSchema.make({
        id: emp.tag,
        position: emp.position,
        type: emp.type,
        firstName: emp.firstName,
        lastName: emp.lastName,
        email: emp.email,
        createdAt: DateTime.unsafeFromDate(emp.createdAt),
        updatedAt: DateTime.unsafeFromDate(emp.updatedAt),
    })
}
