package raft_example

type ReadRequest struct {
    Table  string
    Fields []string
}

type ReadResponse struct {
    Values []string
}

type RedirectResponse struct {
    Leader string
}

type LeaderUnknownResponse struct {
}
