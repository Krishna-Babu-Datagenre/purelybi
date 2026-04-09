# Auth API

Base prefix: `/api/auth`

---

## POST `/api/auth/signup`

Register a new user.

**Request**
```json
{
  "email": "user@example.com",
  "password": "password123",
  "full_name": "Jane Doe"
}
```
- `full_name` is optional (defaults to `""`)
- `password` minimum 8 characters

**Response `201 Created`**
```json
{
  "access_token": "<jwt>",
  "refresh_token": "<jwt>",
  "user": {
    "id": "<uuid>",
    "email": "user@example.com",
    "full_name": "Jane Doe",
    "avatar_url": null,
    "role": "client"
  }
}
```

**Error `400`** — email already registered or invalid input.

> If email confirmation is required, returns `200` with a message asking the user to confirm their email — no tokens are issued.

---

## POST `/api/auth/signin`

Sign in with email and password.

**Request**
```json
{
  "email": "user@example.com",
  "password": "password123"
}
```

**Response `200 OK`** — same shape as signup (`access_token`, `refresh_token`, `user`).

**Error `400`** — invalid credentials.

---

## GET `/api/auth/google`

Get the Google OAuth redirect URL.

**Query param** (optional): `redirect_to=<url>`

**Response `200 OK`**
```json
{ "url": "https://accounts.google.com/o/oauth2/..." }
```

Redirect the user's browser to `url` to begin the Google OAuth flow.

---

## GET `/api/auth/me`

Get the current user's profile.

**Header required:** `Authorization: Bearer <access_token>`

**Response `200 OK`**
```json
{
  "id": "<uuid>",
  "email": "user@example.com",
  "full_name": "Jane Doe",
  "avatar_url": null,
  "role": "client"
}
```

**Error `401`** — missing or invalid token.

---

## Notes

- Store `access_token` in memory (or a short-lived cookie). Use it as `Authorization: Bearer <token>` on all authenticated requests.
- `role` is either `"client"` or `"admin"`.
- Sign-up confirmation emails can be forced to a frontend URL by setting `AUTH_SIGNUP_EMAIL_REDIRECT_TO` in backend env. If unset, Supabase Auth project redirect settings are used.
