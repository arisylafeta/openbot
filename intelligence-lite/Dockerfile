# The self-hosted Intelligence stand-in. Same Bun as the OpenBot server image.
# The workspace lockfile is the root one, so the manifests it names are copied
# before the install even though only this package runs.
FROM oven/bun:1.4.3

WORKDIR /app

COPY package.json bun.lock tsconfig.base.json bunfig.toml ./
COPY app/package.json app/package.json
COPY server/package.json server/package.json
COPY worker/package.json worker/package.json
COPY shared/package.json shared/package.json
COPY toolkit/package.json toolkit/package.json
COPY extension/package.json extension/package.json
COPY intelligence-lite/package.json intelligence-lite/package.json
RUN bun install --frozen-lockfile --production

COPY intelligence-lite/src intelligence-lite/src
COPY intelligence-lite/migrations intelligence-lite/migrations
COPY intelligence-lite/tsconfig.json intelligence-lite/tsconfig.json

USER bun
WORKDIR /app/intelligence-lite
ENV PORT=7100
EXPOSE 7100
CMD ["bun", "src/index.ts"]
