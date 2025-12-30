import { HttpApiBuilder, HttpApiError } from '@effect/platform'
import { WarpApi, PublicEmployeeSchema, EmployeeNotFoundError, PublicEmployee } from '@effect-api-example/api/definition'
import { EmployeeTag } from '@effect-api-example/shared'
import * as PgDrizzle from '@effect/sql-drizzle/Pg'
import { employees } from '../../db/schema/employees'
import { eq, inArray, like, SQL } from 'drizzle-orm'
import { DateTime, Effect } from 'effect'

export const EmployeesGroupLive = HttpApiBuilder.group(WarpApi, 'Employees', (handlers) =>
    Effect.gen(function* () {
        return handlers
            .handle('list', ({ urlParams: { types, limit, afterId, beforeId } }) =>
                Effect.gen(function* () {
                    const db = yield* PgDrizzle.PgDrizzle

                    // Build where conditions
                    const conditions: SQL[] = []

                    if (types && types.length > 0) {
                        conditions.push(inArray(employees.type, types))
                    }

                    let query = db.select().from(employees).$dynamic()

                    for (const condition of conditions) {
                        query = query.where(condition)
                    }

                    const results = yield* query.pipe(
                        Effect.catchAll((e) =>
                            Effect.gen(function* () {
                                yield* Effect.logError('Failed to list employees: ', e)
                                return yield* Effect.fail(new HttpApiError.InternalServerError())
                            }),
                        ),
                    )

                    // Sort by tag for consistent pagination
                    const sortedEmployees = [...results].sort((a, b) => a.tag.localeCompare(b.tag))

                    // Apply cursor-based pagination in memory
                    let startIndex = 0
                    let endIndex = sortedEmployees.length

                    if (afterId) {
                        const cursorIndex = sortedEmployees.findIndex((e) => e.tag === afterId)
                        if (cursorIndex !== -1) {
                            startIndex = cursorIndex + 1
                        }
                    }

                    if (beforeId) {
                        const cursorIndex = sortedEmployees.findIndex((e) => e.tag === beforeId)
                        if (cursorIndex !== -1) {
                            endIndex = cursorIndex
                        }
                    }

                    const sliced = sortedEmployees.slice(startIndex, endIndex)
                    const pageData = sliced.slice(0, limit)
                    const hasMore = sliced.length > limit

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
