import { HttpApi } from '@effect/platform'
import * as Groups from './groups'

// Define the api by adding all groups
export class WarpApi extends HttpApi.make('WarpApi')
  .add(Groups.HealthGroup)
  .add(Groups.EmployeesGroup)
  .prefix('/v1')
{}
