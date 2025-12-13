package main

import (
	"context"
	"net/http"
	"os"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

var (
	jwtSecret    = []byte(getEnv("JWT_SECRET", "default-secret-do-not-use-in-prod"))
	adminApiKey  = getEnv("ADMIN_API_KEY", "admin-secret")
)

type contextKey string

const (
	contextKeyAccountID contextKey = "accountID"
)

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// AuthMiddleware handles JWT authentication.
func (app *App) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// CreateToken does not require a JWT, it requires an Admin Key.
		// We handle that check inside the handler to keep routing simple.
		// However, for all other actions, we expect a valid JWT.

		target := r.Header.Get("X-Amz-Target")
		if strings.HasSuffix(target, ".CreateToken") {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			app.sendErrorResponse(w, "MissingAuthenticationToken", "Request is missing Authentication Token", http.StatusUnauthorized)
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			app.sendErrorResponse(w, "InvalidAuthenticationToken", "Invalid Authentication Token format", http.StatusUnauthorized)
			return
		}

		tokenString := parts[1]
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return jwtSecret, nil
		})

		if err != nil || !token.Valid {
			app.sendErrorResponse(w, "InvalidAuthenticationToken", "Invalid Authentication Token", http.StatusUnauthorized)
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			app.sendErrorResponse(w, "InvalidAuthenticationToken", "Invalid Token Claims", http.StatusUnauthorized)
			return
		}

		accountID, ok := claims["account_id"].(string)
		if !ok || accountID == "" {
			app.sendErrorResponse(w, "InvalidAuthenticationToken", "Token missing account_id", http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), contextKeyAccountID, accountID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
