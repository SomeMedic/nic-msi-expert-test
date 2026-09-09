ARG NODE_IMAGE=node:22.20.0-bookworm-slim@sha256:b21fe589dfbe5cc39365d0544b9be3f1f33f55f3c86c87a76ff65a02f8f5848e
ARG NGINX_IMAGE=nginxinc/nginx-unprivileged:1.30.4-alpine@sha256:442753882674b49ae2c1de83ed67896131c0777f56df5005e356e62bc3f7e7ce

FROM ${NODE_IMAGE} AS build
ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0
WORKDIR /workspace/apps/frontend
RUN corepack enable && corepack prepare pnpm@10.20.0 --activate
COPY apps/frontend/package.json apps/frontend/pnpm-lock.yaml apps/frontend/.npmrc ./
RUN pnpm install --frozen-lockfile
COPY contracts /workspace/contracts
COPY apps/frontend /workspace/apps/frontend
RUN pnpm gen:api && pnpm build

FROM ${NGINX_IMAGE} AS runtime
COPY --from=build /workspace/apps/frontend/dist /usr/share/nginx/html
COPY apps/frontend/nginx.conf /etc/nginx/conf.d/default.conf
USER 101
EXPOSE 8080
