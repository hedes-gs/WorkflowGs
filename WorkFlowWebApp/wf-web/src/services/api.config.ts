import { AxiosRequestConfig } from "axios";

export const ServiceConfig: AxiosRequestConfig = {
    withCredentials: false,
    timeout: 30000,
    headers: {
        common: {
            "Cache-Control": "no-cache, no-store, must-revalidate",
            Pragma: "no-cache",
            "Content-Type": "application/json,application/json+stream ",
            Accept: "application/json,application/json+stream"
        },
    }
    //,    paramsSerializer: (params: PathLike) => qs.stringify(params, { indices: false }),
}
