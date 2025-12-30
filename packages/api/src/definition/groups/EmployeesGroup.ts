import { HttpApiEndpoint, HttpApiError, HttpApiGroup, HttpApiSchema } from '@effect/platform'
import { EmployeeTagSchema, EmployeeTypeSchema, EmailSchema } from '@effect-api-example/shared'
import { Schema as S } from 'effect'
import * as Pagination from '../Pagination.js'
import { ApiKeyAuthMiddleware } from '../middleware/ApiKeyAuthMiddleware.js'

// Public employee schema for API responses
export const PublicEmployeeSchema = S.Struct({
    id: EmployeeTagSchema,
    position: S.String,
    type: EmployeeTypeSchema,
    firstName: S.String,
    lastName: S.String,
    email: EmailSchema,
    createdAt: S.DateTimeUtc,
    updatedAt: S.DateTimeUtc,
})
export type PublicEmployee = typeof PublicEmployeeSchema.Type

// Query parameters for listing employees
const ListEmployeesUrlParamsSchema = S.Struct({
    ...Pagination.ApiQueryParams(EmployeeTagSchema).fields,
    types: S.optional(S.Array(EmployeeTypeSchema)),
})

export class EmployeeNotFoundError extends S.TaggedError<EmployeeNotFoundError>()(
    'EmployeeNotFoundError',
    {
        id: EmployeeTagSchema,
        message: S.String,
    },
    HttpApiSchema.annotations({ status: 404 }),
) {}

export const EmployeesGroup = HttpApiGroup.make('Employees')
    .add(
        HttpApiEndpoint.get('list', '/')
            .setUrlParams(ListEmployeesUrlParamsSchema)
            .addSuccess(Pagination.ApiResponse(PublicEmployeeSchema)),
    )
    .add(
        HttpApiEndpoint.get('get', '/:id')
            .setPath(S.Struct({ id: EmployeeTagSchema }))
            .addSuccess(PublicEmployeeSchema)
            .addError(EmployeeNotFoundError, { status: 404 }),
    )
    .addError(HttpApiError.InternalServerError, { status: 500 })
    .middleware(ApiKeyAuthMiddleware)
    .prefix('/employees')
