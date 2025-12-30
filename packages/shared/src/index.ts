import { Schema as S } from "effect"
import { randomUUID } from "crypto"

export const EmployeeIdSchema = S.Number.pipe(
  S.int(),
  S.brand("EmployeeId")
)
export type EmployeeId = typeof EmployeeIdSchema.Type

// String-based tag for public API use (e.g., "emp_123")
export const EmployeeTagSchema = S.String.pipe(
  S.startsWith('emp_'),
  S.annotations({
    title: 'EmployeeTag',
    description: 'A unique string identifier for an employee (e.g., emp_abc123)',
    examples: ['emp_abc123'],
  }),
  S.brand('EmployeeTag'),
)
export type EmployeeTag = typeof EmployeeTagSchema.Type

// If you made it this far, please don't use this
export const generateEmployeeTag = (): EmployeeTag =>
  EmployeeTagSchema.make(`emp_${randomUUID().replace(/-/g, '')}`)

export const EmployeeTypeSchema = S.Literal(
  'full_time',
  'hourly'
)

export const EmailSchema = S.String.pipe(
    S.pattern(/^(?!\.)(?!.*\.\.)([a-z0-9_'+\-\.]*)[a-z0-9_'+\-]@([a-z0-9][a-z0-9\-]*\.)+[a-z]{2,}$/i),
    S.annotations({
        title: 'Email',
        description: 'An email with a reasonably valid regex (shamelessly taken from zod)',
        examples: ['adam@joinwarp.com'],
    }),
    S.brand('Email'),
)
export type EmailString = typeof EmailSchema.Type
