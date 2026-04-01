# Build the Vite/React frontend
FROM node:20-alpine AS build
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm cache clean --force && npm ci --include=dev --no-audit --no-fund

COPY . .
RUN npm run build

# Serve with nginx and proxy /api to the backend service
FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
