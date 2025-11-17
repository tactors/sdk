package runtime

import (
	"fmt"
	"strings"

	"github.com/tactors/sdk/actors"
)

func workflowQueueFor(kind string, desc *actors.Description) string {
	if desc != nil {
		if name := strings.TrimSpace(desc.WorkflowQueue); name != "" {
			return name
		}
	}
	return fmt.Sprintf("%s-workflow", kind)
}

func activityQueueFor(kind string, desc *actors.Description) string {
	if desc != nil {
		if name := strings.TrimSpace(desc.ActivityQueue); name != "" {
			return name
		}
	}
	return fmt.Sprintf("%s-activity", kind)
}
