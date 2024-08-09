else:
            response["status"] = "failed"
            response["message"] = "All Mandatory jobs are not successful"
            return {
                'statusCode': 500,
            'body': response
            }
