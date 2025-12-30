import { Layer } from 'effect'
import { HealthGroupLive } from './HealthGroupLive'
import { EmployeesGroupLive } from './EmployeesGroupLive'
import { ApiKeyAuthMiddlewareLive } from '../middleware/ApiKeyAuthMiddlewareLive'

export const HttpGroupsLive = Layer.mergeAll(HealthGroupLive, EmployeesGroupLive).pipe(
  Layer.provide(ApiKeyAuthMiddlewareLive)
)
