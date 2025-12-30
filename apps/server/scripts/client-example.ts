import { FetchHttpClient, HttpApiClient, HttpClient, HttpClientRequest } from '@effect/platform'
import { BunContext } from '@effect/platform-bun'
import { WarpApi } from '@effect-api-example/api/definition'
import { Duration, Effect } from 'effect'

const baseUrl = 'http://localhost:9277'
const apiKey = 'test-api-key'

const program = Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient

    const client = yield* HttpApiClient.make(WarpApi, {
        baseUrl,
        transformClient: HttpClient.mapRequest((req) =>
            HttpClientRequest.setHeader(req, 'x-api-key', apiKey),
        ),
    }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient))

    yield* Effect.logInfo('=== Testing Employee API ===')

    // First request: Get first page of employees
    yield* Effect.logInfo('Fetching first page of employees (limit 10)...')
    const page1 = yield* client.Employees.list({ urlParams: { limit: 10 } })
    yield* Effect.logInfo(`Got ${page1.data.length} employees, hasMore: ${page1.hasMore}`)

    if (page1.data.length > 0) {
        yield* Effect.logInfo(`First employee: ${page1.data[0].firstName} ${page1.data[0].lastName}`)
    }

    yield* Effect.sleep(Duration.millis(500))

    // Second request: Get next page using cursor
    if (page1.hasMore && page1.data.length > 0) {
        const lastId = page1.data[page1.data.length - 1].id
        yield* Effect.logInfo(`Fetching next page after cursor: ${lastId}...`)
        const page2 = yield* client.Employees.list({ urlParams: { limit: 10, afterId: lastId } })
        yield* Effect.logInfo(`Got ${page2.data.length} employees, hasMore: ${page2.hasMore}`)

        if (page2.data.length > 0) {
            yield* Effect.logInfo(`First employee on page 2: ${page2.data[0].firstName} ${page2.data[0].lastName}`)
        }
    }

    yield* Effect.sleep(Duration.millis(500))

    // Third request: Filter by employee type
    yield* Effect.logInfo('Fetching full-time employees only...')
    const fullTimeEmployees = yield* client.Employees.list({
        urlParams: { limit: 5, types: ['full_time'] },
    })
    yield* Effect.logInfo(`Got ${fullTimeEmployees.data.length} full-time employees`)

    yield* Effect.sleep(Duration.millis(500))

    // Fourth request: Get a specific employee
    if (page1.data.length > 0) {
        const employeeId = page1.data[0].id
        yield* Effect.logInfo(`Fetching specific employee: ${employeeId}...`)
        const employee = yield* client.Employees.get({ path: { id: employeeId } })
        yield* Effect.logInfo(
            `Employee details: ${employee.firstName} ${employee.lastName}, ${employee.position}, ${employee.email}`,
        )
    }

    yield* Effect.sleep(Duration.millis(500))

    // Fifth request: Test hourly employees
    yield* Effect.logInfo('Fetching hourly employees only...')
    const hourlyEmployees = yield* client.Employees.list({
        urlParams: { limit: 5, types: ['hourly'] },
    })
    yield* Effect.logInfo(`Got ${hourlyEmployees.data.length} hourly employees`)

    yield* Effect.logInfo('=== Client example complete ===')
})

Effect.runPromise(program.pipe(Effect.provide(FetchHttpClient.layer))).catch(console.error)
