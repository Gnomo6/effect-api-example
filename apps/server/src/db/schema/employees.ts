
import { integer, pgEnum, pgTable, timestamp, varchar } from 'drizzle-orm/pg-core'
import { EmployeeTypeSchema, EmployeeId, EmployeeTag, EmailString, generateEmployeeTag } from '@effect-api-example/shared'

export const employeeTypeEnum = pgEnum('employee_type', EmployeeTypeSchema.literals)

export const employees = pgTable('employees', {
    id: integer()
        .$type<EmployeeId>()
        .primaryKey()
        .generatedAlwaysAsIdentity(),
    tag: varchar()
        .$default(() => generateEmployeeTag())
        .$type<EmployeeTag>()
        .notNull()
        .unique(),
    position: varchar().notNull(),
    type: employeeTypeEnum().notNull().default('full_time'),
    email: varchar()
        .$type<EmailString>()
        .notNull()
        .unique(),
    firstName: varchar().notNull(),
    lastName: varchar().notNull(),
    createdAt: timestamp().defaultNow().notNull(),
    updatedAt: timestamp()
        .defaultNow()
        .$onUpdate(() => new Date())
        .notNull(),
})

