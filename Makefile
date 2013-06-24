install:
	go install ./...

fmt:
	gofmt -w *.go
	colcheck *.go

tags:
	find ./ -name '*.go' -and -not -wholename '*_tests/*' -print0 \
		| xargs -0 gotags > TAGS

push:
	git push origin master
	git push github master

