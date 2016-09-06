$namespaceManager = $null
$messagingFactory = $null
$queueClient = $null
$topicClient = $null
$topic = $null
$subscriptionClient = $null

function Connect-SB {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [ValidateNotNullOrEmpty()]
        [System.String] $ConnectionString
    )
    process {
        $script:namespaceManager = [Microsoft.ServiceBus.NamespaceManager]::CreateFromConnectionString($ConnectionString)
        $script:messagingFactory = [Microsoft.ServiceBus.Messaging.MessagingFactory]::CreateFromConnectionString($ConnectionString)
    }
}

function PreFlight {
    if ($null -eq $script:namespaceManager) {
        throw 'Missing connection to Service Bus Namespace, Run Connect-SB'
    }
    if ($null -eq $script:messagingFactory) {
        throw 'Missing connection to Service Bus Namespace, Run Connect-SB'
    }
}

function Get-SBQueue {
    [CmdletBinding(DefaultParameterSetName='List')]
    param (
        [Parameter(ParameterSetName='Named',ValueFromPipeline)]
        [ValidateNotNullOrEmpty()]
        [System.String] $Name
    )
    process {
        PreFlight
        if ($PSCmdlet.ParameterSetName -eq 'Named') {
            $script:namespaceManager.GetQueue($Name)
        } else {
            $script:namespaceManager.GetQueues()
        }
    }
}

function Select-SBQueue {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [Microsoft.ServiceBus.Messaging.QueueDescription] $Queue
    )
    process {
        PreFlight
        $script:queueClient = $script:messagingFactory.CreateQueueClient($Queue.Path)
    }
}

function Get-SBTopic {
    [CmdletBinding(DefaultParameterSetName='List')]
    param (
        [Parameter(ParameterSetName='Named',ValueFromPipeline)]
        [ValidateNotNullOrEmpty()]
        [System.String] $Name
    )
    process {
        PreFlight
        if ($PSCmdlet.ParameterSetName -eq 'Named') {
            $script:namespaceManager.GetTopic($Name)
        } else {
            $script:namespaceManager.GetTopics()
        }
    }
}

function Select-SBTopic {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [Microsoft.ServiceBus.Messaging.TopicDescription] $Topic
    )
    process {
        $script:topicClient = $script:messagingFactory.CreateTopicClient($Topic.Path)
        $script:topic = $Topic
    }
}

function New-SBQueue {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [ValidateNotNullOrEmpty()]
        [System.String] $Name
    )
    process {
        PreFlight
        $script:namespaceManager.CreateQueue($Name)
    }
}

function New-SBTopic {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [ValidateNotNullOrEmpty()]
        [System.String] $Name
    )
    process {
        PreFlight
        $script:namespaceManager.CreateTopic($Name)
    }
}

function Remove-SBQueue {
    [CmdletBinding(ConfirmImpact='High',SupportsShouldProcess)]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [Microsoft.ServiceBus.Messaging.QueueDescription] $Queue
    )
    process {
        PreFlight
        if ($PSCmdlet.ShouldProcess($Queue.Path)) {
            $script:namespaceManager.DeleteQueue($Queue.Path)
        }
    }
}

function Remove-SBTopic {
    [CmdletBinding(ConfirmImpact='High',SupportsShouldProcess)]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [Microsoft.ServiceBus.Messaging.TopicDescription] $Topic
    )
    process {
        PreFlight
        if ($PSCmdlet.ShouldProcess($Topic.Path)) {
            $script:namespaceManager.DeleteTopic($Topic.Path)
        }
    }
}

function Send-SBQueueMessage {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [ValidateNotNull()]
        [psobject] $Message
    )
    process {
        PreFlight
        $message = New-Object -TypeName 'Microsoft.ServiceBus.Messaging.BrokeredMessage' -ArgumentList $Message
        $Script:queueClient.Send($Message)
    }
}

function Receive-SBQueueMessage {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [ScriptBlock]$Action
    )
    process {
        PreFlight
        $message = $Script:queueClient.Receive()
        try {
            $method = ([Microsoft.ServiceBus.Messaging.BrokeredMessage].GetMethods() | ?{$_.Name -eq 'GetBody' -and $_.GetParameters().Count -eq 0})
            $gMethod = $method.MakeGenericMethod([psobject]) 
            $msg = $gMethod.Invoke($message,$null)
            $null = Add-Member -InputObject $message -MemberType NoteProperty -Name body -Value $msg
            
            # Invoke the action with the parameter $message
            # How do we check if the provided scriptblock contains a 
            # Message parameter?
            & $Action -Message $message

            # If the action did not throw an exception the Message
            # we assume that the client code has processed the Message
            # The message can be set to complete.
            $message.Complete()
        }
        catch [System.Exception] {
            # Removes the peeklock
            $message.Abandon()            
        }
    }
}

function New-SBTopicSubscription {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [Microsoft.ServiceBus.Messaging.TopicDescription] $Topic,
        
        [Parameter(Mandatory)]
        [ValidateNotNullOrEmpty()]
        [System.String] $Name
    )
    process {
        PreFlight
        $script:namespaceManager.CreateSubscription($Topic.Path, $Name)
    }
}

function Get-SBTopicSubscription {
    [OutputType([Microsoft.ServiceBus.Messaging.SubscriptionDescription])]
    [CmdletBinding(DefaultParameterSetName='List')]
    param (
        [Parameter(ValueFromPipeline)]
        [Microsoft.ServiceBus.Messaging.TopicDescription] $Topic,
        
        [Parameter(Mandatory,ParameterSetName='Named')]
        [ValidateNotNullOrEmpty()]
        [System.String] $Name
    )
    process {
        PreFlight
        if (-not $Topic) {
            $T = $script:topic
        } else {
            $T = $Topic
        }
        
        if ($PSCmdlet.ParameterSetName -eq 'Named') {
            $script:namespaceManager.GetSubscription($T.Path, $Name)
        } else {
            $script:namespaceManager.GetSubscriptions($T.Path)
        }
    }
}

function Select-SBTopicSubscription {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [Microsoft.ServiceBus.Messaging.SubscriptionDescription] $Subscription
    )
    process {
        PreFlight
        $script:subscriptionClient = $script:messagingFactory.CreateSubscriptionClient($Subscription.TopicPath, $Subscription.Name)
    }
}

function Send-SBTopicMessage {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [ValidateNotNull()]
        [psobject] $Message
    )
    process {
        PreFlight
        $message = New-Object -TypeName 'Microsoft.ServiceBus.Messaging.BrokeredMessage' -ArgumentList $Message
        $Script:topicClient.Send($Message)
    }
}

function Receive-SBTopicSubscriptionMessage {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [scriptblock]$Action
    )
    process {
        PreFlight
        $Message = $script:subscriptionClient.Receive()
        try {
            $method = ([Microsoft.ServiceBus.Messaging.BrokeredMessage].GetMethods() | ?{$_.Name -eq 'GetBody' -and $_.GetParameters().Count -eq 0})
            $gMethod = $method.MakeGenericMethod([psobject]) 
            $msg = $gMethod.Invoke($message,$null)
            $null = Add-Member -InputObject $message -MemberType NoteProperty -Name body -Value $msg
            
            # Invoke the action with the parameter $message
            # How do we check if the provided scriptblock contains a 
            # Message parameter?
            & $Action -Message $message

            # If the action did not throw an exception the Message
            # we assume that the client code has processed the Message
            # The message can be set to complete.
            $message.Complete()
        }
        catch [System.Exception] {
            # Removes the peeklock
            $message.Abandon()            
        }
    }
}