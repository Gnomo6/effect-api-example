import { faker } from '@faker-js/faker'
import { EmailSchema, EmployeeTypeSchema } from '@effect-api-example/shared'
import { Effect } from 'effect'
import * as PgDrizzle from '@effect/sql-drizzle/Pg'
import { employees } from '../src/db/schema/employees'
import { SqlLive } from '../src/db/SqlLive'

const EMPLOYEE_COUNT = 1000

const positions = [
    'Software Engineer',
    'Senior Software Engineer',
    'Staff Engineer',
    'Engineering Manager',
    'Product Manager',
    'Designer',
    'Data Analyst',
    'DevOps Engineer',
    'QA Engineer',
    'Technical Writer',
]


const seed = Effect.gen(function* () {
    const db = yield* PgDrizzle.PgDrizzle

    yield* Effect.logInfo(`Seeding ${EMPLOYEE_COUNT} employees...`)

    const employeeData = Array.from({ length: EMPLOYEE_COUNT }, () => ({
        firstName: faker.person.firstName(),
        lastName: faker.person.lastName(),
        email: EmailSchema.make(faker.internet.email().toLowerCase()),
        position: faker.helpers.arrayElement(positions),
        type: faker.helpers.arrayElement(EmployeeTypeSchema.literals),
    }))

    const batchSize = 100
    for (let i = 0; i < employeeData.length; i += batchSize) {
        const batch = employeeData.slice(i, i + batchSize)
        yield* db.insert(employees).values(batch)
        yield* Effect.logInfo(`Inserted ${Math.min(i + batchSize, EMPLOYEE_COUNT)} / ${EMPLOYEE_COUNT}`)
    }

    yield* Effect.logInfo('Seeding complete!')
})

const program = seed.pipe(Effect.provide(SqlLive()))

Effect.runPromise(program).catch(console.error)
