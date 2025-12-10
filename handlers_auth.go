package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/tabeth/concreteq/models"
	"github.com/tabeth/concreteq/store"
)

// CreateTokenHandler handles requests to generate a new JWT token.
// It requires a valid Admin API Key.
func (app *App) CreateTokenHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Verify Admin API Key
	if r.Header.Get("X-Admin-Key") != adminApiKey {
		app.sendErrorResponse(w, "AccessDenied", "Invalid or missing Admin API Key", http.StatusForbidden)
		return
	}

	var req models.CreateTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.AccountId == "" {
		app.sendErrorResponse(w, "MissingParameter", "AccountId is required", http.StatusBadRequest)
		return
	}

	// 2. Generate JWT
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"account_id": req.AccountId,
	})

	tokenString, err := token.SignedString(jwtSecret)
	if err != nil {
		app.sendErrorResponse(w, "InternalFailure", "Failed to sign token", http.StatusInternalServerError)
		return
	}

	resp := models.CreateTokenResponse{
		Token: tokenString,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// checkPermission evaluates if the caller has permission to perform the action on the queue.
func (app *App) checkPermission(ctx context.Context, queueName, action string) error {
	callerID, ok := ctx.Value(contextKeyAccountID).(string)
	if !ok || callerID == "" {
		return &SqsError{Type: "AccessDenied", Message: "Access to the resource is denied. Missing authentication."}
	}

	// Fetch Queue Attributes to get Owner and Policy
	attrs, err := app.Store.GetQueueAttributes(ctx, queueName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			return &SqsError{Type: "QueueDoesNotExist", Message: "The specified queue does not exist."}
		}
		return err
	}

	// 1. Owner Check: If caller is owner, allow.
	owner, ok := attrs["QueueOwnerAWSAccountId"]
	if ok && owner == callerID {
		return nil
	}
	// If owner attribute is missing (legacy queues?), we might default to deny or allow.
	// For production readiness, strict deny is safer.

	// 2. Policy Check
	policyStr, hasPolicy := attrs["Policy"]
	if !hasPolicy {
		return &SqsError{Type: "AccessDenied", Message: "Access to the resource is denied."}
	}

	var policy struct {
		Statement []struct {
			Effect    string      `json:"Effect"`
			Principal interface{} `json:"Principal"` // Can be map or string ("*")
			Action    interface{} `json:"Action"`    // Can be string or []string
			Sid       string      `json:"Sid"`
		} `json:"Statement"`
	}

	if err := json.Unmarshal([]byte(policyStr), &policy); err != nil {
		// If policy is corrupt, deny.
		return &SqsError{Type: "AccessDenied", Message: "Access to the resource is denied. Invalid Policy."}
	}

	for _, stmt := range policy.Statement {
		if stmt.Effect != "Allow" {
			continue // We only support Allow for now (Deny is implicit)
		}

		// Check Principal
		principalMatch := false
		if strPrincipal, ok := stmt.Principal.(string); ok && strPrincipal == "*" {
			principalMatch = true
		} else if mapPrincipal, ok := stmt.Principal.(map[string]interface{}); ok {
			if aws, ok := mapPrincipal["AWS"]; ok {
				if awsStr, ok := aws.(string); ok {
					if awsStr == "*" || awsStr == callerID {
						principalMatch = true
					}
				} else if awsSlice, ok := aws.([]interface{}); ok {
					for _, id := range awsSlice {
						if idStr, ok := id.(string); ok && idStr == callerID {
							principalMatch = true
							break
						}
					}
				}
			}
		}

		if !principalMatch {
			continue
		}

		// Check Action
		actionMatch := false
		if strAction, ok := stmt.Action.(string); ok {
			if checkActionMatch(strAction, action) {
				actionMatch = true
			}
		} else if sliceAction, ok := stmt.Action.([]interface{}); ok {
			for _, a := range sliceAction {
				if aStr, ok := a.(string); ok {
					if checkActionMatch(aStr, action) {
						actionMatch = true
						break
					}
				}
			}
		}

		if actionMatch {
			return nil // Allowed
		}
	}

	return &SqsError{Type: "AccessDenied", Message: "Access to the resource is denied."}
}

func checkActionMatch(policyAction, requestAction string) bool {
	if policyAction == "*" || policyAction == "SQS:*" {
		return true
	}
	// "SQS:SendMessage" vs "SendMessage"
	// Normalize to simple action name
	policySimple := strings.TrimPrefix(policyAction, "SQS:")
	if policySimple == requestAction {
		return true
	}
	return false
}
