import { HttpApiBuilder, HttpApiScalar, HttpLayerRouter, HttpMiddleware, HttpServer, HttpServerResponse, OpenApi } from '@effect/platform'
import { BunHttpServer, BunRuntime } from '@effect/platform-bun'
import { WarpApi } from '@effect-api-example/api/definition'
import { Layer } from 'effect'
import * as SqlLive  from './db/SqlLive'
import { HttpGroupsLive } from './api/groups'
import { ApiKeyAuthMiddlewareLive } from './api/middleware/ApiKeyAuthMiddlewareLive'
import { NodeSdk } from '@effect/opentelemetry'
import { BatchSpanProcessor, ConsoleSpanExporter } from '@opentelemetry/sdk-trace-base'


// The Live implementation of the API groups has a dependency on
// `PGDrizzle`, so we provide it here
const ApiImplementationLive = HttpGroupsLive.pipe(
    Layer.provide(SqlLive.layer)
)

// `WarpApi` requires an implementation, so we provide that here
const HttpApiRoutes = HttpLayerRouter.addHttpApi(WarpApi).pipe(
    Layer.provide(ApiImplementationLive),
)

// expose swagger docs  at /docs
const DocsRoute = HttpApiScalar.layerHttpLayerRouter({
    api: WarpApi,
    path: '/docs',
})

// expose openapi schema
const OpenApiJsonRoute = HttpLayerRouter.add(
    'GET',
    '/docs/openapi.json',
    HttpServerResponse.json(OpenApi.fromApi(WarpApi)),
).pipe(Layer.provide(HttpLayerRouter.layer))


// merge all routes
const AllRoutes = Layer.mergeAll(
    HttpApiRoutes,
    DocsRoute,
    OpenApiJsonRoute,
).pipe(Layer.provide(HttpLayerRouter.cors()))

/*
 * Tracing
 * @see {https://effect.website/docs/observability/tracing/}
 */
const OTELNodeSdk = NodeSdk.layer(() => ({
    resource: { serviceName: 'warp-api'},
    spanProcessor: new BatchSpanProcessor(new ConsoleSpanExporter())
}))


// run the server
const ServerLive = HttpLayerRouter.serve(AllRoutes).pipe(
    Layer.provide(BunHttpServer.layer({ port: 9277 })),
    Layer.provide(OTELNodeSdk)
)

BunRuntime.runMain(Layer.launch(ServerLive))
