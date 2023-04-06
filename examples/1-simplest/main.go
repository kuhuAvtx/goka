package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers                 = []string{"172.31.13.207:9092"}
	readerTopic goka.Stream = "apr3_test"
	writerTopic goka.Stream = "apr3_test_out"
	group       goka.Group  = "apr3_test_grp"

	tmc *goka.TopicManagerConfig
)

func init() {
	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}

// emits a single message and leave
func runEmitter() {
	emitter, err := goka.NewEmitter(brokers, readerTopic, new(codec.String))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish()
	err = emitter.EmitSync("some-key", "some-value")
	if err != nil {
		log.Fatalf("error emitting message: %v", err)
	}
	log.Println("message emitted")
}

// emits a single message and leave
func runFinalWriter(key string, msg interface{}) {
	emitter, err := goka.NewEmitter(brokers, writerTopic, new(codec.String))
	if err != nil {
		log.Fatalf("runFinalWriter: error creating emitter: %v", err)
	}
	defer emitter.Finish()
	err = emitter.EmitSync(key, msg)
	if err != nil {
		log.Fatalf("error emitting message: %v", err)
	}
	log.Println("message emitted")
}

// ____Example of message incoming from goflow2_____
// 2023/03/21 13:11:36 key = [127 0 0 1]-, counter = 198404, msg = {"Type":"NETFLOW_V9","ObservationPointID":0,"ObservationDomainID":0,"TimeReceived":1679404260,"SequenceNum":161338694,"SamplingRate":0,"SamplerAddress":"127.0.0.1","TimeFlowStart":1679404257,"TimeFlowEnd":1679404257,"TimeFlowStartMs":1679404256008,"TimeFlowEndMs":1679404256008,"Bytes":0,"Packets":0,"SrcAddr":"192.152.220.119","DstAddr":"204.24.202.178","Etype":2048,"Proto":6,"SrcPort":58443,"DstPort":80,"InIf":0,"OutIf":0,"SrcMac":"00:00:00:00:00:00","DstMac":"00:00:00:00:00:00","SrcVlan":0,"DstVlan":0,"VlanId":0,"IngressVrfID":0,"EgressVrfID":0,"IPTos":0,"ForwardingStatus":0,"IPTTL":0,"TCPFlags":0,"IcmpType":0,"IcmpCode":0,"IPv6FlowLabel":0,"FragmentId":0,"FragmentOffset":0,"BiFlowDirection":0,"SrcAS":0,"DstAS":0,"NextHop":"","NextHopAS":0,"SrcNet":0,"DstNet":0,"EtypeName":"IPv4","ProtoName":"TCP","IcmpName":""}

// process messages until ctrl-c is pressed
func runProcessor() {
	// process callback is invoked for each message delivered from
	// "example-stream" topic.
	cb := func(ctx goka.Context, msg interface{}) {
		var counter int64
		// ctx.Value() gets from the group table the value that is stored for
		// the message's key.
		if val := ctx.Value(); val != nil {
			counter = val.(int64)
		}
		counter++
		// SetValue stores the incremented counter in the group table for in
		// the message's key.
		ctx.SetValue(counter)
		result := make(map[string]interface{})
		json.Unmarshal([]byte(msg.(string)), &result)
		dstPort := result["DstPort"]
		result["Bytes"] = rand.Intn(1000000)
		if dstPort != nil && dstPort.(float64) < 1024 {
			log.Printf("key = %s, counter = %v, msg = %v", ctx.Key(), counter, result)
			resJs, _ := json.Marshal(result)
			runFinalWriter(ctx.Key(), string(resJs))
		}
	}

	// Define a new processor group. The group defines all inputs, outputs, and
	// serialization formats. The group-table topic is "example-group-table".
	g := goka.DefineGroup(group,
		goka.Input(readerTopic, new(codec.String), cb),
		goka.Persist(new(codec.Int64)),
	)

	p, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
	)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			log.Printf("error running processor: %v", err)
		}
	}()

	sigs := make(chan os.Signal)
	go func() {
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	}()

	select {
	case <-sigs:
	case <-done:
	}
	cancel()
	<-done
}

func main() {
	config := goka.DefaultConfig()
	// since the emitter only emits one message, we need to tell the processor
	// to read from the beginning
	// As the processor is slower to start than the emitter, it would not consume the first
	// message otherwise.
	// In production systems however, check whether you really want to read the whole topic on first start, which
	// can be a lot of messages.
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	goka.ReplaceGlobalConfig(config)

	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	defer tm.Close()
	err = tm.EnsureStreamExists(string(readerTopic), 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", readerTopic, err)
	}

	runEmitter()   // emits one message and stops
	runProcessor() // press ctrl-c to stop
}
