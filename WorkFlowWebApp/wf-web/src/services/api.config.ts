
export const ServiceConfig = {
    returnRejectedPromiseOnError: true,
    withCredentials: false,
    timeout: 30000,
	baseURL: "http://localhost:8080/",
    headers: {
        common: {
            "Cache-Control": "no-cache, no-store, must-revalidate",
            Pragma: "no-cache",
            "Content-Type": "application/json",
			Accept: "application/json",
			"Access-Control-Allow-Origin": "http://localhost:8080"
        },
	}
	//,    paramsSerializer: (params: PathLike) => qs.stringify(params, { indices: false }),
}