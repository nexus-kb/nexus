FROM node:24-bookworm-slim AS build

WORKDIR /build/WebUI

RUN corepack enable && corepack prepare pnpm@11.19.0 --activate

COPY WebUI/package.json WebUI/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

COPY WebUI/ ./
RUN pnpm build

FROM nginx:1.29-alpine

COPY WebUI/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /build/WebUI/dist /usr/share/nginx/html

EXPOSE 80
